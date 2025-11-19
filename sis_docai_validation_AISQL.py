import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, current_timestamp, sql_expr
import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime
import pypdfium2 as pdfium
import io

st.set_page_config(layout="wide", page_title="📋 DocAI Invoice Validation", page_icon="📋")

# --- Configuration ---
DB_NAME = "DOC_AI_QS_DB"
SCHEMA_NAME = "DOC_AI_SCHEMA"
STAGE_NAME = "DOC_AI_STAGE"

INVOICE_INFO_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.INVOICE_INFO"
SILVER_VALIDATED_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.SILVER_VALIDATED_INVOICES"
VW_PENDING_VALIDATIONS = f"{DB_NAME}.{SCHEMA_NAME}.VW_PENDING_VALIDATIONS"
VW_VALIDATION_STATS = f"{DB_NAME}.{SCHEMA_NAME}.VW_VALIDATION_STATS"

# Initialize session state
if 'current_invoice' not in st.session_state:
    st.session_state.current_invoice = None
if 'validator_name' not in st.session_state:
    st.session_state.validator_name = ""
if 'pdf_page' not in st.session_state:
    st.session_state.pdf_page = 0
if 'pdf_doc' not in st.session_state:
    st.session_state.pdf_doc = None
if 'pdf_url' not in st.session_state:
    st.session_state.pdf_url = None

# --- Get Snowflake Session ---
try:
    session = get_active_session()
    st.success("❄️ Snowflake session established!")
    CURRENT_USER = session.get_current_role().replace("\"", "")
except Exception as e:
    st.error(f"Error getting Snowflake session: {e}")
    st.stop()

# --- PDF Display Functions ---
def display_pdf_page():
    """Renders and displays the current PDF page."""
    if 'pdf_doc' not in st.session_state or st.session_state['pdf_doc'] is None:
        st.warning("No PDF document loaded.")
        return
    if 'pdf_page' not in st.session_state:
        st.session_state['pdf_page'] = 0

    pdf = st.session_state['pdf_doc']
    page_index = st.session_state['pdf_page']
    num_pages = len(pdf)

    if not 0 <= page_index < num_pages:
        st.error(f"Invalid page index: {page_index}. Must be between 0 and {num_pages-1}.")
        st.session_state['pdf_page'] = 0
        page_index = 0

    page = pdf[page_index]

    try:
        bitmap = page.render(scale=2, rotation=0)
        pil_image = bitmap.to_pil()
        st.image(pil_image, use_container_width='always')
    except Exception as e:
        st.error(f"Error rendering PDF page {page_index + 1}: {e}")

def previous_pdf_page():
    """Navigates to the previous PDF page."""
    if 'pdf_page' in st.session_state and st.session_state['pdf_page'] > 0:
        st.session_state['pdf_page'] -= 1

def next_pdf_page():
    """Navigates to the next PDF page."""
    if ('pdf_page' in st.session_state and
        'pdf_doc' in st.session_state and
        st.session_state['pdf_doc'] is not None and
        st.session_state['pdf_page'] < len(st.session_state['pdf_doc']) - 1):
        st.session_state['pdf_page'] += 1

def analyze_invoice_with_cortex(invoice_no, file_name):
    """Analyze invoice using Snowflake Cortex AISQL functions"""
    try:
        # Use stage path directly for PARSE_DOCUMENT (not presigned URL)
        stage_path = f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{file_name}"
        
        # First extract text from the PDF using PARSE_DOCUMENT
        parse_query = f"""
        SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT('{stage_path}') as document_text
        """
        
        doc_result = session.sql(parse_query).collect()
        if not doc_result or not doc_result[0]['DOCUMENT_TEXT']:
            return {"error": "Could not extract text from PDF"}
            
        document_text = str(doc_result[0]['DOCUMENT_TEXT'])
        
        # Clean and truncate text for analysis
        if len(document_text) > 3000:
            document_text = document_text[:3000] + "..."
        
        # Escape quotes for SQL safety
        clean_text = document_text.replace("'", "''")
        
        # 1. Language Detection using AI_COMPLETE
        language_query = f"""
        SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
            'llama3.1-8b',
            'Identify the language of this invoice text. Respond with only the language name in English (e.g., English, French, German, Spanish): {clean_text[:800]}'
        ) as detected_language
        """
        
        # 2. Country Detection using AI_COMPLETE  
        country_query = f"""
        SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
            'llama3.1-8b',
            'Identify the country where this invoice was issued based on address, tax information, or other indicators. Respond with only the country name: {clean_text[:800]}'
        ) as detected_country
        """
        
        # 3. Currency Detection using AI_COMPLETE
        currency_query = f"""
        SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
            'llama3.1-8b',
            'Identify the currency used in this invoice. Look for currency symbols or codes. Respond with only the 3-letter currency code (e.g., EUR, USD, GBP): {clean_text[:800]}'
        ) as detected_currency
        """
        
        # 4. Invoice Category Classification using AI_CLASSIFY
        category_query = f"""
        SELECT SNOWFLAKE.CORTEX.AI_CLASSIFY(
            '{clean_text[:1500]}',
            ['Equipment', 'Services', 'Clothing', 'Transport', 'Rental', 'Software', 'Office Supplies', 'Travel', 'Utilities', 'Marketing', 'Other']
        ) as invoice_category
        """
        
        # 5. Business Department Classification using AI_CLASSIFY
        department_query = f"""
        SELECT SNOWFLAKE.CORTEX.AI_CLASSIFY(
            '{clean_text[:1500]}',
            ['Operations', 'HR', 'Finance', 'IT', 'Marketing', 'Sales', 'Legal', 'Procurement', 'Facilities', 'Other']
        ) as business_department
        """
        
        # Execute queries with error handling
        results = {}
        
        try:
            language_result = session.sql(language_query).collect()
            results['language'] = language_result[0]['DETECTED_LANGUAGE'].strip() if language_result else "Unknown"
        except Exception as e:
            results['language'] = f"Error: {str(e)[:50]}"
        
        try:
            country_result = session.sql(country_query).collect()
            results['country'] = country_result[0]['DETECTED_COUNTRY'].strip() if country_result else "Unknown"
        except Exception as e:
            results['country'] = f"Error: {str(e)[:50]}"
            
        try:
            currency_result = session.sql(currency_query).collect()
            results['currency'] = currency_result[0]['DETECTED_CURRENCY'].strip() if currency_result else "Unknown"
        except Exception as e:
            results['currency'] = f"Error: {str(e)[:50]}"
            
        try:
            category_result = session.sql(category_query).collect()
            results['category'] = category_result[0]['INVOICE_CATEGORY'] if category_result else "Other"
        except Exception as e:
            results['category'] = f"Error: {str(e)[:50]}"
            
        try:
            department_result = session.sql(department_query).collect()
            results['department'] = department_result[0]['BUSINESS_DEPARTMENT'] if department_result else "Other"
        except Exception as e:
            results['department'] = f"Error: {str(e)[:50]}"
        
        # Add metadata
        results['document_length'] = len(document_text)
        results['stage_path'] = stage_path
        
        return results
        
    except Exception as e:
        return {"error": f"Analysis failed: {str(e)}"}

def get_invoice_analysis_summary():
    """Get summary of AI analysis across all invoices"""
    try:
        # This would typically come from a table where analysis results are stored
        # For now, we'll return a placeholder
        summary_query = f"""
        SELECT 
            COUNT(*) as total_analyzed,
            'Multiple' as languages_detected,
            'Various' as countries_detected,
            'Mixed' as currencies_detected
        FROM {DB_NAME}.{SCHEMA_NAME}.INVOICE_INFO
        """
        
        result = session.sql(summary_query).collect()
        if result:
            return {
                "total_analyzed": result[0]['TOTAL_ANALYZED'],
                "languages": result[0]['LANGUAGES_DETECTED'],
                "countries": result[0]['COUNTRIES_DETECTED'],
                "currencies": result[0]['CURRENCIES_DETECTED']
            }
        return {"total_analyzed": 0, "languages": "None", "countries": "None", "currencies": "None"}
        
    except Exception as e:
        st.error(f"Error getting analysis summary: {str(e)}")
        return {"total_analyzed": 0, "languages": "Error", "countries": "Error", "currencies": "Error"}

# --- Helper Functions ---
@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_validation_stats():
    """Get validation statistics"""
    try:
        stats_df = session.table(VW_VALIDATION_STATS).to_pandas()
        return stats_df.iloc[0] if not stats_df.empty else None
    except Exception as e:
        st.error(f"Error loading validation stats: {str(e)}")
        return None

@st.cache_data(ttl=300)
def get_pending_validations(status_filter='All'):
    """Load pending validations"""
    try:
        query = f"""
        SELECT 
            invoice_no,
            customer_no,
            invoice_date,
            total_amount,
            cost_center,
            file_name,
            file_size,
            last_modified,
            snowflake_file_url,
            current_status,
            validated_by,
            validated_timestamp,
            change_summary
        FROM {VW_PENDING_VALIDATIONS}
        WHERE current_status = '{status_filter}' OR '{status_filter}' = 'All'
        ORDER BY 
            CASE WHEN current_status = 'PENDING' THEN 1 ELSE 2 END,
            last_modified DESC
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error loading pending validations: {str(e)}")
        return pd.DataFrame()

def get_pdf_presigned_url(file_name):
    """Get presigned URL for PDF viewing using Snowpark session"""
    try:
        query = f"SELECT GET_PRESIGNED_URL('@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}', '{file_name}', 3600) AS URL"
        result = session.sql(query).to_pandas()
        return result.iloc[0]['URL'] if not result.empty else None
    except Exception as e:
        st.error(f"Error getting PDF URL: {str(e)}")
        return None

def validate_invoice_procedure(invoice_no, file_name, validated_by, new_values, validation_notes, validation_status):
    """Call the validation stored procedure"""
    try:
        # Prepare parameters with proper null handling and escaping
        customer_param = f"'{new_values.get('customer_no')}'" if new_values.get('customer_no') else 'NULL'
        date_param = f"'{new_values.get('invoice_date')}'" if new_values.get('invoice_date') else 'NULL'
        amount_param = str(new_values.get('total_amount')) if new_values.get('total_amount') is not None else 'NULL'
        cost_center_param = f"'{new_values.get('cost_center')}'" if new_values.get('cost_center') else 'NULL'
        notes_param = f"'{validation_notes.replace(chr(39), chr(39)+chr(39))}'" if validation_notes else 'NULL'
        
        query = f"""
        CALL {DB_NAME}.{SCHEMA_NAME}.SP_VALIDATE_INVOICE(
            '{invoice_no}',
            '{file_name}',
            '{validated_by}',
            {customer_param},
            {date_param},
            {amount_param},
            {cost_center_param},
            {notes_param},
            '{validation_status}'
        )
        """
        
        result = session.sql(query).collect()
        return result[0][0] if result else "Validation completed"
    except Exception as e:
        st.error(f"Error validating invoice: {str(e)}")
        return None

def load_pdf_from_stage(file_name):
    """Load PDF from Snowflake stage"""
    if not file_name:
        return False
        
    stage_path = f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{file_name}"
    
    # Check if we need to load a new PDF
    if st.session_state['pdf_url'] != stage_path:
        st.write(f"Loading PDF from: {stage_path}")
        try:
            pdf_stream = session.file.get_stream(stage_path, decompress=False)
            pdf_bytes = pdf_stream.read()
            pdf_stream.close()
            st.session_state['pdf_doc'] = pdfium.PdfDocument(pdf_bytes)
            st.session_state['pdf_url'] = stage_path
            st.session_state['pdf_page'] = 0
            st.success(f"Loaded '{file_name}'")
            return True
        except Exception as e:
            st.warning(f"Failed to load PDF '{file_name}': {str(e)}")
            st.session_state['pdf_doc'] = None
            st.session_state['pdf_url'] = None
            return False
    return True

# --- Upload Section ---
with st.sidebar:
    st.header("📂 Upload Document")
    uploaded = st.file_uploader("PDF / Image", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True)
    st.divider()
    upload_placeholder = st.empty()

if uploaded:
    for uploaded_file in uploaded:
        upload_placeholder.empty()
        file_bytes = uploaded_file.read()
        file_name = uploaded_file.name
        
        with st.spinner(f"Processing {file_name}..."):
            try:
                # Stage file
                session.file.put_stream(
                    io.BytesIO(file_bytes), 
                    f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{file_name}", 
                    overwrite=True, 
                    auto_compress=False
                )
                
                # Refresh stage to update directory table
                session.sql(f"ALTER STAGE {DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME} REFRESH").collect()
                
                # Manually trigger DocAI extraction task for immediate processing
                try:
                    session.sql(f"EXECUTE TASK {DB_NAME}.{SCHEMA_NAME}.DOCAI_EXTRACT").collect()
                    st.success(f"✅ {file_name} uploaded and processed!")
                    
                    # Small delay to allow processing to complete
                    import time
                    time.sleep(2)
                    
                except Exception as task_error:
                    st.warning(f"⚠️ {file_name} uploaded, but extraction task failed: {str(task_error)}")
                    st.info("The file will be processed automatically within 1 minute.")
                
                # Clear caches to show new data immediately
                st.cache_data.clear()
                
            except Exception as e:
                st.error(f"❌ Failed to upload {file_name}: {str(e)}")
        
        with st.sidebar:
            if st.button("🔄 Refresh Queue", key=f"refresh_{file_name}"):
                st.cache_data.clear()
                st.rerun()

# --- Main App UI ---
st.title("📋 DocAI Invoice Validation System")
st.markdown(f"Connected as role: **{CURRENT_USER}**")

# --- Sidebar Stats ---
with st.sidebar:
    st.header("📊 Validation Statistics")
    stats = get_validation_stats()
    if stats is not None:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Invoices", int(stats['TOTAL_INVOICES']))
            st.metric("Pending", int(stats['PENDING_COUNT']))
        with col2:
            st.metric("Validated", int(stats['VALIDATED_COUNT']))
            st.metric("Validation Rate", f"{stats['VALIDATION_RATE']}%")
        
        if stats['CORRECTIONS_MADE'] > 0:
            st.metric("Corrections Made", int(stats['CORRECTIONS_MADE']))
    
    st.divider()
    
    # User info
    st.header("👤 Validator Info")
    validator_name = st.text_input(
        "Your Name", 
        value=st.session_state.validator_name,
        key="validator_input"
    )
    # Only update session state if the value actually changed
    if validator_name != st.session_state.validator_name:
        st.session_state.validator_name = validator_name

# --- Main Content Tabs ---
tab1, tab2, tab3 = st.tabs(["🔍 Validation Queue", "📈 Dashboard", "🤖 AI Analysis"])

with tab1:
    st.header("Invoice Validation Queue")
    
    # Filter controls
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        status_filter = st.selectbox(
            "Filter by Status", 
            ["All", "PENDING", "VALIDATED", "REJECTED"],
            key="status_filter"
        )
    with col2:
        search_invoice = st.text_input("Search Invoice No", key="search_invoice")
    with col3:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    # Load validations
    validations_df = get_pending_validations(status_filter)
    
    if validations_df.empty:
        st.info("No invoices found matching the filter criteria.")
        
        # Check if there are any invoices in INVOICE_INFO but not showing due to filters
        try:
            total_invoices_query = f"SELECT COUNT(*) as total FROM {DB_NAME}.{SCHEMA_NAME}.INVOICE_INFO"
            total_count = session.sql(total_invoices_query).collect()[0]['TOTAL']
            if total_count > 0:
                st.info(f"📊 Found {total_count} total invoices in the system. Try adjusting your filters or refresh the data.")
                if st.button("🔄 Show All Invoices", key="show_all"):
                    st.session_state.status_filter = "All"
                    st.cache_data.clear()
                    st.rerun()
        except:
            pass
            
    else:
        # Apply search filter
        if search_invoice:
            validations_df = validations_df[
                validations_df['INVOICE_NO'].str.contains(search_invoice, case=False, na=False)
            ]
        
        st.subheader(f"📋 Invoices ({len(validations_df)} items)")
        
        # Invoice selection
        if not validations_df.empty:
            # Create selection dropdown
            invoice_options = [""] + [
                f"{row['INVOICE_NO']} - {row['FILE_NAME']} ({row['CURRENT_STATUS']})"
                for _, row in validations_df.iterrows()
            ]
            
            selected_option = st.selectbox(
                "Select Invoice to Validate",
                invoice_options,
                key="invoice_selector"
            )
            
            if selected_option and selected_option != "":
                # Parse selection to get invoice
                selected_idx = invoice_options.index(selected_option) - 1
                selected_invoice = validations_df.iloc[selected_idx]
                st.session_state.current_invoice = selected_invoice
                
                # Display validation interface
                if st.session_state.current_invoice is not None:
                    invoice = st.session_state.current_invoice
                    
                    st.divider()
                    st.subheader(f"📝 Validating Invoice: {invoice['INVOICE_NO']}")
                    
                    # Two columns: PDF viewer and validation form
                    col1, col2 = st.columns([1, 1])
                    
                    with col1:
                        # PDF Preview
                        st.markdown("### 📄 PDF Preview")
                        
                        if load_pdf_from_stage(invoice['FILE_NAME']):
                            if st.session_state.get('pdf_doc') is not None:
                                # Navigation controls
                                nav_col1, nav_col2, nav_col3 = st.columns([1, 2, 1])
                                with nav_col1:
                                    st.button("⏮️ Previous", on_click=previous_pdf_page, use_container_width=True)
                                with nav_col2:
                                    st.write(
                                        f"<div style='text-align: center;'>Page {st.session_state['pdf_page'] + 1} of {len(st.session_state['pdf_doc'])}</div>", 
                                        unsafe_allow_html=True
                                    )
                                with nav_col3:
                                    st.button("Next ⏭️", on_click=next_pdf_page, use_container_width=True)
                                
                                display_pdf_page()
                            else:
                                st.info("Could not load document preview.")
                        else:
                            st.error("Cannot load PDF preview")
                            st.info(f"File: {invoice['FILE_NAME']}")
                    
                    with col2:
                        # Validation Form
                        st.markdown("### ✅ Validation Form")
                        
                        with st.form("validation_form"):
                            st.markdown("**DocAI Extracted Values (Editable):**")
                            
                            # Display original values and allow editing
                            col_a, col_b = st.columns(2)
                            
                            with col_a:
                                st.text_input(
                                    "Invoice No", 
                                    value=invoice['INVOICE_NO'], 
                                    disabled=True
                                )
                                new_customer_no = st.text_input(
                                    "Customer No", 
                                    value=invoice['CUSTOMER_NO'] or '', 
                                    key="new_customer_no"
                                )
                                new_invoice_date = st.text_input(
                                    "Invoice Date", 
                                    value=invoice['INVOICE_DATE'] or '', 
                                    key="new_invoice_date"
                                )
                            
                            with col_b:
                                new_total_amount = st.number_input(
                                    "Total Amount", 
                                    value=float(invoice['TOTAL_AMOUNT']) if invoice['TOTAL_AMOUNT'] else 0.0,
                                    format="%.2f", 
                                    key="new_total_amount"
                                )
                                new_cost_center = st.text_input(
                                    "Cost Center", 
                                    value=invoice['COST_CENTER'] or '', 
                                    key="new_cost_center"
                                )
                            
                            # File info (read-only)
                            st.markdown("**File Information:**")
                            st.text_input("File Name", value=invoice['FILE_NAME'], disabled=True)
                            
                            # Validation notes
                            validation_notes = st.text_area(
                                "Validation Notes", 
                                placeholder="Add any comments about this validation...",
                                key="validation_notes"
                            )
                            
                            # Validation actions
                            st.markdown("**Validation Decision:**")
                            col_btn1, col_btn2, col_btn3 = st.columns(3)
                            
                            with col_btn1:
                                validate_btn = st.form_submit_button("✅ Validate & Approve", type="primary")
                            with col_btn2:
                                reject_btn = st.form_submit_button("❌ Reject", type="secondary")
                            with col_btn3:
                                save_draft_btn = st.form_submit_button("💾 Save as Draft")
                            
                            # Process form submission
                            if validate_btn or reject_btn or save_draft_btn:
                                if not st.session_state.validator_name:
                                    st.error("Please enter your name in the sidebar first.")
                                else:
                                    # Determine changes
                                    new_values = {}
                                    if new_customer_no != (invoice['CUSTOMER_NO'] or ''):
                                        new_values['customer_no'] = new_customer_no
                                    if new_invoice_date != (invoice['INVOICE_DATE'] or ''):
                                        new_values['invoice_date'] = new_invoice_date
                                    if new_total_amount != (float(invoice['TOTAL_AMOUNT']) if invoice['TOTAL_AMOUNT'] else 0.0):
                                        new_values['total_amount'] = new_total_amount
                                    if new_cost_center != (invoice['COST_CENTER'] or ''):
                                        new_values['cost_center'] = new_cost_center
                                    
                                    # Determine validation status
                                    if validate_btn:
                                        validation_status = "VALIDATED"
                                    elif reject_btn:
                                        validation_status = "REJECTED"
                                    else:  # save_draft_btn
                                        validation_status = "PENDING"
                                    
                                    # Submit validation
                                    with st.spinner("Processing validation..."):
                                        result = validate_invoice_procedure(
                                            invoice['INVOICE_NO'],
                                            invoice['FILE_NAME'],
                                            st.session_state.validator_name,
                                            new_values,
                                            validation_notes,
                                            validation_status
                                        )
                                    
                                    if result:
                                        if validation_status == "VALIDATED":
                                            st.success(f"✅ Invoice {invoice['INVOICE_NO']} validated successfully!")
                                        elif validation_status == "REJECTED":
                                            st.warning(f"❌ Invoice {invoice['INVOICE_NO']} rejected.")
                                        else:
                                            st.info(f"💾 Draft saved for invoice {invoice['INVOICE_NO']}.")
                                        
                                        if new_values:
                                            st.info(f"Changes made to: {', '.join(new_values.keys())}")
                                        
                                        # Clear cache and reset current invoice (form submission will refresh automatically)
                                        st.cache_data.clear()
                                        st.session_state.current_invoice = None
                                        # Removed st.rerun() to prevent infinite refresh loop
            
            # Show summary table
            st.divider()
            st.subheader("📋 Invoice Summary")
            display_df = validations_df[[
                'INVOICE_NO', 'CUSTOMER_NO', 'INVOICE_DATE', 'TOTAL_AMOUNT', 
                'COST_CENTER', 'FILE_NAME', 'CURRENT_STATUS'
            ]].copy()
            display_df.columns = ['Invoice No', 'Customer', 'Date', 'Amount', 'Cost Center', 'File', 'Status']
            st.dataframe(display_df, use_container_width=True)

with tab2:
    st.header("📈 Validation Dashboard")
    
    # Reload stats for dashboard
    stats = get_validation_stats()
    if stats is not None:
        # Progress metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Invoices", int(stats['TOTAL_INVOICES']))
        with col2:
            st.metric(
                "Validated", 
                int(stats['VALIDATED_COUNT']), 
                delta=f"{stats['VALIDATION_RATE']}% complete"
            )
        with col3:
            st.metric("Pending", int(stats['PENDING_COUNT']))
        with col4:
            st.metric("Corrections Made", int(stats['CORRECTIONS_MADE']))
        
        # Progress bar
        progress = stats['VALIDATION_RATE'] / 100.0
        st.progress(progress)
        st.caption(f"Validation Progress: {stats['VALIDATION_RATE']}%")
    
    # Recent validations
    st.subheader("🕐 Recent Validations")
    try:
        recent_query = f"""
        SELECT 
            invoice_no,
            validation_status,
            validated_by,
            validated_timestamp,
            changes_made,
            change_summary
        FROM {SILVER_VALIDATED_TABLE}
        WHERE validation_status IN ('VALIDATED', 'REJECTED')
        ORDER BY validated_timestamp DESC
        LIMIT 10
        """
        recent_df = session.sql(recent_query).to_pandas()
        
        if not recent_df.empty:
            recent_df.columns = ['Invoice No', 'Status', 'Validated By', 'Timestamp', 'Changes Made', 'Summary']
            st.dataframe(recent_df, use_container_width=True)
        else:
            st.info("No recent validations found.")
    except Exception as e:
        st.error(f"Error loading recent validations: {str(e)}")

with tab3:
    st.header("🤖 AI Invoice Analysis")
    st.markdown("Analyze invoices using Snowflake Cortex AISQL to extract business insights")
    
    # AI Analysis Summary
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📊 Analysis Overview")
        summary = get_invoice_analysis_summary()
        
        st.metric("Total Invoices", summary['total_analyzed'])
        st.metric("Languages Detected", summary['languages'])
        st.metric("Countries Detected", summary['countries'])
        st.metric("Currencies Detected", summary['currencies'])
    
    with col2:
        st.subheader("🔍 Analyze Individual Invoice")
        
        # Load available invoices for analysis
        available_invoices = get_pending_validations('All')
        
        if not available_invoices.empty:
            invoice_options = ["Select an invoice..."] + [
                f"{row['INVOICE_NO']} - {row['FILE_NAME']}"
                for _, row in available_invoices.iterrows()
            ]
            
            selected_analysis_option = st.selectbox(
                "Choose Invoice for AI Analysis",
                invoice_options,
                key="analysis_invoice_selector"
            )
            
            if selected_analysis_option != "Select an invoice...":
                # Parse selection
                selected_idx = invoice_options.index(selected_analysis_option) - 1
                selected_invoice = available_invoices.iloc[selected_idx]
                
                if st.button("🔍 Analyze with Cortex AI", type="primary"):
                    with st.spinner("Analyzing invoice with Cortex AISQL..."):
                        analysis_results = analyze_invoice_with_cortex(
                            selected_invoice['INVOICE_NO'], 
                            selected_invoice['FILE_NAME']
                        )
                    
                    if "error" in analysis_results:
                        st.error(f"Analysis failed: {analysis_results['error']}")
                    else:
                        st.success("✅ AI Analysis Complete!")
                        
                        # Display results in organized columns
                        st.divider()
                        st.subheader(f"📋 Analysis Results: {selected_invoice['INVOICE_NO']}")
                        
                        col_a, col_b, col_c = st.columns(3)
                        
                        with col_a:
                            st.markdown("**🌍 Geographic & Language**")
                            st.info(f"**Language:** {analysis_results['language']}")
                            st.info(f"**Country:** {analysis_results['country']}")
                            st.info(f"**Currency:** {analysis_results['currency']}")
                        
                        with col_b:
                            st.markdown("**📦 Invoice Classification**")
                            st.success(f"**Category:** {analysis_results['category']}")
                            
                            # Show category with emoji
                            category_emojis = {
                                "Equipment": "🔧", "Services": "🛠️", "Clothing": "👔",
                                "Transport": "🚛", "Rental": "🏠", "Software": "💻",
                                "Office Supplies": "📝", "Travel": "✈️", "Utilities": "💡",
                                "Marketing": "📢", "Other": "📄"
                            }
                            emoji = category_emojis.get(analysis_results['category'], "📄")
                            st.markdown(f"### {emoji} {analysis_results['category']}")
                        
                        with col_c:
                            st.markdown("**🏢 Department Assignment**")
                            st.success(f"**Department:** {analysis_results['department']}")
                            
                            # Show department with emoji
                            dept_emojis = {
                                "Operations": "⚙️", "HR": "👥", "Finance": "💰",
                                "IT": "💻", "Marketing": "📈", "Sales": "🤝",
                                "Legal": "⚖️", "Procurement": "🛒", "Facilities": "🏢",
                                "Other": "📋"
                            }
                            emoji = dept_emojis.get(analysis_results['department'], "📋")
                            st.markdown(f"### {emoji} {analysis_results['department']}")
                        
                        # Additional analysis details
                        st.divider()
                        with st.expander("📊 Analysis Details"):
                            st.write(f"**Document Length:** {analysis_results['document_length']} characters")
                            st.write("**AI Models Used:**")
                            st.write("- Language/Country/Currency: `llama3.1-8b` via AI_COMPLETE")
                            st.write("- Category Classification: AI_CLASSIFY")
                            st.write("- Department Classification: AI_CLASSIFY")
                            st.write("- Document Parsing: PARSE_DOCUMENT")
                            
                            st.info("💡 **Tip:** These AI insights can help with automated expense categorization, compliance checking, and budget allocation.")
        else:
            st.info("No invoices available for analysis. Please upload some invoices first.")
    
    # AI Analysis Features Info
    st.divider()
    st.subheader("🚀 Cortex AISQL Features Used")
    
    feature_cols = st.columns(4)
    
    with feature_cols[0]:
        st.markdown("""
        **AI_COMPLETE**
        - Language detection
        - Country identification  
        - Currency extraction
        """)
    
    with feature_cols[1]:
        st.markdown("""
        **AI_CLASSIFY**
        - Invoice categorization
        - Department assignment
        - Multi-label classification
        """)
    
    with feature_cols[2]:
        st.markdown("""
        **PARSE_DOCUMENT**
        - PDF text extraction
        - Layout preservation
        - OCR capabilities
        """)
    
    with feature_cols[3]:
        st.markdown("""
        **Benefits**
        - Automated insights
        - Consistent categorization
        - Enhanced compliance
        """)

# --- Status message ---
if st.session_state.validator_name:
    st.sidebar.success(f"Logged in as: {st.session_state.validator_name}")
else:
    st.sidebar.warning("Please enter your name to start validating invoices.") 