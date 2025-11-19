import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

st.set_page_config(
    page_title="🚀 AI-Powered Invoice Intelligence", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuration ---
DB_NAME = "DOC_AI_QS_DB"
SCHEMA_NAME = "DOC_AI_SCHEMA"

# Table references
RECONCILE_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_ITEMS"
RECONCILE_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_TOTALS"
BRONZE_TRANSACT_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.TRANSACT_ITEMS"
BRONZE_TRANSACT_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS"
BRONZE_DOCAI_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOCAI_INVOICE_ITEMS"
BRONZE_DOCAI_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOCAI_INVOICE_TOTALS"
GOLD_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.GOLD_INVOICE_ITEMS"
GOLD_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.GOLD_INVOICE_TOTALS"

# Initialize session state
session_state_keys = [
    'processed_invoice_id', 'cached_mismatch_summary', 'chat_history',
    'ai_insights_cache', 'edited_transact_items', 'edited_transact_totals',
    'docai_items', 'docai_totals'
]

for key in session_state_keys:
    if key not in st.session_state:
        if key == 'chat_history':
            st.session_state[key] = []
        elif key == 'ai_insights_cache':
            st.session_state[key] = {}
        else:
            st.session_state[key] = None

# --- Snowflake Connection ---
@st.cache_resource
def get_snowflake_connection():
    """Get Snowflake connection for SiS"""
    return st.connection("snowflake")

conn = get_snowflake_connection()

# Get current user for audit trail
try:
    current_user_result = conn.query("SELECT CURRENT_USER() as user")
    CURRENT_USER = current_user_result.iloc[0]['USER']
except:
    CURRENT_USER = "unknown_user"

# --- Reconciliation Functions ---
@st.cache_data(ttl=600)
def load_reconcile_data(status_filter='Pending Review'):
    """Loads data from reconciliation tables, optionally filtering by review_status."""
    try:
        # Use UNION ALL to get all invoice IDs needing review from both tables
        query = f"""
        SELECT DISTINCT invoice_id, review_status, last_reconciled_timestamp
        FROM {RECONCILE_ITEMS_TABLE}
        WHERE review_status = '{status_filter}' OR '{status_filter}' = 'All'
        UNION
        SELECT DISTINCT invoice_id, review_status, last_reconciled_timestamp
        FROM {RECONCILE_TOTALS_TABLE}
        WHERE review_status = '{status_filter}' OR '{status_filter}' = 'All'
        ORDER BY last_reconciled_timestamp DESC
        """
        reconcile_df = conn.query(query)

        # Load full details for display
        if status_filter != 'All':
            reconcile_items_full_df = conn.query(f"SELECT * FROM {RECONCILE_ITEMS_TABLE} WHERE review_status = '{status_filter}'")
            reconcile_totals_full_df = conn.query(f"SELECT * FROM {RECONCILE_TOTALS_TABLE} WHERE review_status = '{status_filter}'")
        else:
            reconcile_items_full_df = conn.query(f"SELECT * FROM {RECONCILE_ITEMS_TABLE}")
            reconcile_totals_full_df = conn.query(f"SELECT * FROM {RECONCILE_TOTALS_TABLE}")

        return reconcile_df, reconcile_items_full_df, reconcile_totals_full_df
    except Exception as e:
        st.error(f"Error loading reconcile data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def load_bronze_data(invoice_id):
    """Loads data from all relevant bronze tables for a specific invoice_id."""
    bronze_data = {}
    try:
        if invoice_id:
            bronze_data['transact_items'] = conn.query(f"SELECT * FROM {BRONZE_TRANSACT_ITEMS_TABLE} WHERE invoice_id = '{invoice_id}'")
            bronze_data['transact_totals'] = conn.query(f"SELECT * FROM {BRONZE_TRANSACT_TOTALS_TABLE} WHERE invoice_id = '{invoice_id}'")
            bronze_data['docai_items'] = conn.query(f"SELECT * FROM {BRONZE_DOCAI_ITEMS_TABLE} WHERE invoice_id = '{invoice_id}'")
            bronze_data['docai_totals'] = conn.query(f"SELECT * FROM {BRONZE_DOCAI_TOTALS_TABLE} WHERE invoice_id = '{invoice_id}'")
        return bronze_data
    except Exception as e:
        st.error(f"Error loading Bronze data for invoice {invoice_id}: {e}")
        return {}

def get_invoice_reconciliation_metrics():
    """Get reconciliation metrics from the database."""
    sql_query = f"""
    SELECT
        COUNT(DISTINCT tt.invoice_id) AS total_invoice_count,
        SUM(tt.total) AS grand_total_amount,
        COUNT(DISTINCT CASE
                        WHEN EXISTS (SELECT 1 FROM {GOLD_TOTALS_TABLE} git WHERE git.invoice_id = tt.invoice_id)
                         AND EXISTS (SELECT 1 FROM {GOLD_ITEMS_TABLE} gii WHERE gii.invoice_id = tt.invoice_id)
                        THEN tt.invoice_id
                        ELSE NULL
                    END) AS reconciled_invoice_count,
        COUNT(DISTINCT CASE
                        WHEN EXISTS (SELECT 1 FROM {GOLD_TOTALS_TABLE} git WHERE git.invoice_id = tt.invoice_id AND git.reviewed_by = 'Auto-reconciled')
                         AND EXISTS (SELECT 1 FROM {GOLD_ITEMS_TABLE} gii WHERE gii.invoice_id = tt.invoice_id AND gii.reviewed_by = 'Auto-reconciled')
                        THEN tt.invoice_id
                        ELSE NULL
                    END) AS auto_reconciled_invoice_count,
        SUM(CASE
                WHEN EXISTS (SELECT 1 FROM {GOLD_TOTALS_TABLE} git WHERE git.invoice_id = tt.invoice_id)
                 AND EXISTS (SELECT 1 FROM {GOLD_ITEMS_TABLE} gii WHERE gii.invoice_id = tt.invoice_id)
                THEN tt.total
                ELSE 0
            END) AS total_reconciled_amount
    FROM {BRONZE_TRANSACT_TOTALS_TABLE} AS tt
    """
    
    try:
        result = conn.query(sql_query)
        if not result.empty:
            row = result.iloc[0]
            
            total_invoices = row['TOTAL_INVOICE_COUNT'] or 0
            grand_total = row['GRAND_TOTAL_AMOUNT'] or 0.0
            reconciled_invoices = row['RECONCILED_INVOICE_COUNT'] or 0
            total_reconciled = row['TOTAL_RECONCILED_AMOUNT'] or 0.0
            count_auto_reconciled = row['AUTO_RECONCILED_INVOICE_COUNT'] or 0
            
            reconciled_invoice_ratio = (float(reconciled_invoices) / float(total_invoices)) if total_invoices > 0 else 0.0
            reconciled_amount_ratio = (float(total_reconciled) / float(grand_total)) if grand_total != 0 else 0.0
            
            return {
                'total_invoice_count': int(total_invoices),
                'grand_total_amount': float(grand_total),
                'reconciled_invoice_count': int(reconciled_invoices),
                'total_reconciled_amount': float(total_reconciled),
                'reconciled_invoice_ratio': float(reconciled_invoice_ratio),
                'reconciled_amount_ratio': float(reconciled_amount_ratio),
                'count_auto_reconciled': int(count_auto_reconciled)
            }
    except Exception as e:
        st.error(f"Error getting reconciliation metrics: {e}")
        return None

def summarize_mismatch_details(reconcile_items_details_df, reconcile_totals_details_df, selected_invoice_id):
    """Summarizes item mismatch details using Snowflake Cortex."""
    try:
        # Filter DataFrames for the selected invoice ID
        try:
            item_mismatch_details = reconcile_items_details_df[reconcile_items_details_df["INVOICE_ID"] == selected_invoice_id]["ITEM_MISMATCH_DETAILS"].iloc[0]
        except:
            item_mismatch_details = ""
        
        try:
            total_mismatch_details = reconcile_totals_details_df[reconcile_totals_details_df["INVOICE_ID"] == selected_invoice_id]["ITEM_MISMATCH_DETAILS"].iloc[0]
        except:
            total_mismatch_details = ""

        all_mismatch_details = str(item_mismatch_details) + str(total_mismatch_details)

        if not all_mismatch_details or all_mismatch_details == "":
            return f"No item mismatch details found for Invoice ID: {selected_invoice_id}."

        # Construct the prompt for Snowflake Cortex
        prompt = f"""
        Based on the following item mismatch details for invoice {selected_invoice_id}, please provide a concise summary of the differences.
        Focus on the types of mismatches and affected items or amounts, do not use the words expected or actual.

        Mismatch Details:
        ---
        {all_mismatch_details}
        ---
        """

        # Call Cortex complete function
        try:
            response = conn.query(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as summary")
            summary = response.iloc[0]["SUMMARY"] if not response.empty else "Could not retrieve summary."
        except Exception as e:
            return f"Error calling Snowflake Cortex: {e}"

        return summary

    except Exception as e:
        st.error(f"An error occurred: {e}")
        return f"Failed to generate summary due to an error: {str(e)}"

# --- Enhanced Functions from previous version ---
@st.cache_data(ttl=300)
def get_invoice_metrics():
    """Get basic invoice metrics"""
    try:
        query = f"""
        SELECT 
            COUNT(DISTINCT invoice_id) as total_invoices,
            SUM(total) as total_amount,
            AVG(total) as avg_amount,
            MIN(invoice_date) as earliest_date,
            MAX(invoice_date) as latest_date
        FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
        """
        return conn.query(query).iloc[0].to_dict()
    except Exception as e:
        st.error(f"Error getting metrics: {e}")
        return {}

def ai_fraud_analysis(invoice_id):
    """AI-powered fraud analysis"""
    try:
        query = f"""
        WITH stats AS (
            SELECT 
                AVG(total) as avg_total,
                STDDEV(total) as stddev_total
            FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
        ),
        invoice_data AS (
            SELECT 
                invoice_id,
                total,
                invoice_date,
                (total - stats.avg_total) / stats.stddev_total as z_score
            FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS, stats
            WHERE invoice_id = '{invoice_id}'
        )
        SELECT 
            SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-8b',
                'Analyze this invoice for fraud risk: ID=' || invoice_id || 
                ', Amount=$' || total || ', Date=' || invoice_date || 
                ', Z-score=' || z_score || 
                '. Provide risk assessment and reasoning in 2-3 sentences.'
            ) as analysis
        FROM invoice_data
        """
        result = conn.query(query)
        return result.iloc[0]['ANALYSIS'] if not result.empty else "Analysis not available"
    except Exception as e:
        return f"Error in fraud analysis: {e}"

def ai_categorize_invoice(invoice_id):
    """AI-powered invoice categorization"""
    try:
        query = f"""
        WITH invoice_details AS (
            SELECT 
                t.invoice_id,
                t.total,
                LISTAGG(ti.product_name, ', ') as products
            FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS t
            LEFT JOIN {DB_NAME}.{SCHEMA_NAME}.TRANSACT_ITEMS ti ON t.invoice_id = ti.invoice_id
            WHERE t.invoice_id = '{invoice_id}'
            GROUP BY t.invoice_id, t.total
        )
        SELECT 
            SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-8b',
                'Categorize this invoice based on products: ' || products || 
                '. Amount: $' || total || 
                '. Provide category (e.g., Office Supplies, Food, Technology) and brief explanation.'
            ) as category
        FROM invoice_details
        """
        result = conn.query(query)
        return result.iloc[0]['CATEGORY'] if not result.empty else "Category not available"
    except Exception as e:
        return f"Error in categorization: {e}"

def ai_assistant_query(user_question):
    """AI assistant for answering questions about invoice data"""
    try:
        # Get basic context about the data
        context_query = f"""
        SELECT 
            COUNT(*) as total_invoices,
            SUM(total) as total_amount,
            AVG(total) as avg_amount,
            MIN(invoice_date) as earliest_date,
            MAX(invoice_date) as latest_date
        FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
        """
        context = conn.query(context_query).iloc[0]
        
        prompt = f"""
        You are an AI assistant helping with invoice analysis. Here's the current data context:
        - Total invoices: {context['TOTAL_INVOICES']}
        - Total amount: ${context['TOTAL_AMOUNT']:,.2f}
        - Average amount: ${context['AVG_AMOUNT']:,.2f}
        - Date range: {context['EARLIEST_DATE']} to {context['LATEST_DATE']}
        
        User question: {user_question}
        
        Provide a helpful response based on this invoice data. If you need specific data to answer, suggest what analysis could be done.
        """
        
        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', $${prompt}$$) as response
        """
        result = conn.query(query)
        return result.iloc[0]['RESPONSE'] if not result.empty else "I couldn't process your question."
    except Exception as e:
        return f"Error processing question: {e}"

# --- UI Functions ---
def show_dashboard():
    """Show main dashboard"""
    st.header("🏠 AI Invoice Intelligence Dashboard")
    
    # Get metrics
    metrics = get_invoice_metrics()
    reconciliation_metrics = get_invoice_reconciliation_metrics()
    
    if metrics:
        # Main metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Invoices", f"{metrics.get('TOTAL_INVOICES', 0):,}")
        with col2:
            st.metric("Total Amount", f"${metrics.get('TOTAL_AMOUNT', 0):,.2f}")
        with col3:
            st.metric("Average Amount", f"${metrics.get('AVG_AMOUNT', 0):,.2f}")
        with col4:
            if reconciliation_metrics:
                st.metric("Auto-Reconciled", f"{reconciliation_metrics.get('count_auto_reconciled', 0)}")
    
    # Reconciliation metrics
    if reconciliation_metrics:
        st.subheader("📊 Reconciliation Status")
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "Reconciled Invoices", 
                f"{reconciliation_metrics['reconciled_invoice_ratio']:.1%}",
                delta=f"{reconciliation_metrics['reconciled_invoice_count']}/{reconciliation_metrics['total_invoice_count']}"
            )
        with col2:
            st.metric(
                "Reconciled Amount", 
                f"{reconciliation_metrics['reconciled_amount_ratio']:.1%}",
                delta=f"${reconciliation_metrics['total_reconciled_amount']:,.0f}/${reconciliation_metrics['grand_total_amount']:,.0f}"
            )
    
    # System status
    st.subheader("⚙️ System Status")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.success("✅ Streamlit in Snowflake")
    with col2:
        st.success("✅ Document AI Active")
    with col3:
        try:
            conn.query("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', 'test')")
            st.success("✅ Cortex AI Ready")
        except:
            st.warning("⚠️ Cortex AI Limited")

def show_reconciliation():
    """Show invoice reconciliation interface"""
    st.header("🔍 Invoice Reconciliation")
    
    # Get reconciliation metrics
    with st.spinner("Loading reconciliation metrics..."):
        reconciliation_data = get_invoice_reconciliation_metrics()
    
    if reconciliation_data:
        st.success("Metrics displayed below (updated automatically).")
        st.success(f"{reconciliation_data['count_auto_reconciled']} Invoices out of {reconciliation_data['total_invoice_count']} were fully Auto-Reconciled with DocAI")
        
        st.subheader("Reconciliation Ratios")
        col1, col2 = st.columns(2)
        col1.metric(
            label="Reconciled Invoices (Count Ratio)",
            value=f"{reconciliation_data['reconciled_invoice_ratio']:.2%}",
            help=f"Percentage of unique invoices from TRANSACT_TOTALS found in both GOLD tables. ({reconciliation_data['reconciled_invoice_count']}/{reconciliation_data['total_invoice_count']})"
        )
        col2.metric(
            label="Reconciled Amount (Value Ratio)",
            value=f"{reconciliation_data['reconciled_amount_ratio']:.2%}",
            help=f"Percentage of total amount from TRANSACT_TOTALS that corresponds to reconciled invoices. (${reconciliation_data['total_reconciled_amount']:,.2f} / ${reconciliation_data['grand_total_amount']:,.2f})"
        )

        st.subheader("Detailed Numbers")
        df_metrics = pd.DataFrame([
             {"Metric": "Total Unique Invoices", "Value": reconciliation_data['total_invoice_count']},
             {"Metric": "Fully Reconciled Invoices", "Value": reconciliation_data['reconciled_invoice_count']},
             {"Metric": "Grand Total Amount ($)", "Value": f"{reconciliation_data['grand_total_amount']:,.2f}"},
             {"Metric": "Total Reconciled Amount ($)", "Value": f"{reconciliation_data['total_reconciled_amount']:,.2f}"},
        ]).set_index("Metric")
        st.dataframe(df_metrics)
    else:
        st.warning("Could not retrieve or calculate reconciliation metrics.")

    # Section 1: Display reconcile Tables & Select Invoice
    st.header("1. Invoices Awaiting Review")

    review_status_options = ['Pending Review', 'Reviewed', 'Auto-reconciled']
    selected_status = st.selectbox("Filter by Review Status:", review_status_options, index=0)

    # Load distinct invoice IDs based on filter
    invoices_to_review_df, reconcile_items_details_df, reconcile_totals_details_df = load_reconcile_data(selected_status)

    if not invoices_to_review_df.empty:
        st.write(f"Found {len(invoices_to_review_df['INVOICE_ID'].unique())} unique invoices with totals or items status '{selected_status}'.")

        # Allow user to select an invoice
        invoice_list = [""] + invoices_to_review_df['INVOICE_ID'].unique().tolist()
        selected_invoice_id = st.selectbox(
            "Select Invoice ID to Review/Correct:",
            invoice_list,
            index=0,
            key="invoice_selector"
        )

        # Display details from reconcile tables for context
        with st.expander("Show Reconciliation Details for All Filtered Invoices"):
             st.subheader("Item Reconciliation Details")
             st.dataframe(reconcile_items_details_df, use_container_width=True)
             st.subheader("Total Reconciliation Details")
             st.dataframe(reconcile_totals_details_df, use_container_width=True)

    else:
        st.info(f"No invoices found with status '{selected_status}'.")
        selected_invoice_id = None

    # Section 2: Display Bronze Data for Selected Invoice
    st.header("2. Review and Correct Invoice Data")

    if selected_invoice_id:
        st.subheader(f"Displaying Data for Invoice: `{selected_invoice_id}`")
        
        # Generate mismatch summary
        if selected_invoice_id != st.session_state.processed_invoice_id:
            mismatch_summary = summarize_mismatch_details(reconcile_items_details_df, reconcile_totals_details_df, selected_invoice_id)
            st.session_state.cached_mismatch_summary = mismatch_summary
            st.session_state.processed_invoice_id = selected_invoice_id
        
        if st.session_state.cached_mismatch_summary is not None:
            st.subheader(f"{st.session_state.cached_mismatch_summary}")
        
        # Load data from Bronze layer
        bronze_data_dict = load_bronze_data(selected_invoice_id)

        if bronze_data_dict:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Original Data (Source: TRANSACT_*)**")
                st.markdown(":rainbow[Editable Fields] - These can be edited directly and then accepted in section 3.")

                # Editable Transact Items
                st.write("**Items (Original DB):**")
                if not bronze_data_dict['transact_items'].empty:
                     # Convert relevant columns to numeric for proper editing
                     for col_name in ['QUANTITY', 'UNIT_PRICE', 'TOTAL_PRICE']:
                        if col_name in bronze_data_dict['transact_items'].columns:
                            bronze_data_dict['transact_items'][col_name] = pd.to_numeric(bronze_data_dict['transact_items'][col_name], errors='coerce')

                     edited_transact_items_df = st.data_editor(
                        bronze_data_dict['transact_items'],
                        key="editor_transact_items",
                        num_rows="dynamic",
                        use_container_width=True,
                        column_config={
                            "UNIT_PRICE": st.column_config.NumberColumn(format="$%.2f"),
                            "TOTAL_PRICE": st.column_config.NumberColumn(format="$%.2f"),
                        }
                    )
                     st.session_state.edited_transact_items = edited_transact_items_df
                else:
                     st.info("No data found in TRANSACT_ITEMS for this invoice.")

                # Editable Transact Totals
                st.write("**Totals (Original DB):**")
                if not bronze_data_dict['transact_totals'].empty:
                    transact_totals_edit_df = bronze_data_dict['transact_totals'].head(1).copy()

                    # Convert relevant columns to numeric/date
                    for col_name in ['SUBTOTAL', 'TAX', 'TOTAL']:
                         if col_name in transact_totals_edit_df.columns:
                            transact_totals_edit_df[col_name] = pd.to_numeric(transact_totals_edit_df[col_name], errors='coerce')
                    if 'INVOICE_DATE' in transact_totals_edit_df.columns:
                        transact_totals_edit_df['INVOICE_DATE'] = pd.to_datetime(transact_totals_edit_df['INVOICE_DATE'], errors='coerce').dt.date

                    edited_transact_totals_df = st.data_editor(
                        transact_totals_edit_df,
                        key="editor_transact_totals",
                        use_container_width=True,
                        column_config={
                            "SUBTOTAL": st.column_config.NumberColumn(format="$%.2f"),
                            "TAX": st.column_config.NumberColumn(format="$%.2f"),
                            "TOTAL": st.column_config.NumberColumn(format="$%.2f"),
                            "INVOICE_DATE": st.column_config.DateColumn(),
                        }
                    )
                    st.session_state.edited_transact_totals = edited_transact_totals_df
                else:
                    st.info("No data found in TRANSACT_TOTALS for this invoice.")

            with col2:
                st.markdown("**Document AI Extracted Data**")
                st.markdown("Reference data extracted by Document AI")

                # DocAI Items (Read-only)
                st.write("**Items (DocAI Extracted):**")
                if not bronze_data_dict['docai_items'].empty:
                    st.dataframe(bronze_data_dict['docai_items'], use_container_width=True)
                    st.session_state.docai_items = bronze_data_dict['docai_items']
                else:
                    st.info("No DocAI items data found for this invoice.")

                # DocAI Totals (Read-only)
                st.write("**Totals (DocAI Extracted):**")
                if not bronze_data_dict['docai_totals'].empty:
                    st.dataframe(bronze_data_dict['docai_totals'], use_container_width=True)
                    st.session_state.docai_totals = bronze_data_dict['docai_totals']
                else:
                    st.info("No DocAI totals data found for this invoice.")

        # Section 3: Submit Corrections
        st.header("3. Submit Review and Corrections")
        
        submit_docai_button = st.button("❄️ ✅ Accept DocAI Extracted Values for Reconciliation")
        
        st.write("Or...")
        
        # Add fields for notes and corrected invoice number
        review_notes = st.text_area("Manual Review Notes / Comments:", key="review_notes")

        submit_button = st.button("✍️ ✔️ Accept Manual Edits above for Reconciliation")

        if submit_button or submit_docai_button:
            # Data Validation
            valid = True
            if submit_docai_button:
                if 'docai_items' not in st.session_state or st.session_state.docai_items.empty:
                    st.warning("No item data to submit.")
                    valid = False
                elif 'docai_totals' not in st.session_state or st.session_state.docai_totals.empty:
                    st.warning("No total data to submit.")
                    valid = False
                else:
                    gold_items_df = st.session_state.docai_items[["INVOICE_ID", "PRODUCT_NAME", "QUANTITY", "UNIT_PRICE", "TOTAL_PRICE"]].copy()
                    gold_totals_df = st.session_state.docai_totals[["INVOICE_ID", "INVOICE_DATE", "SUBTOTAL", "TAX", "TOTAL"]].copy()
                
            else:
                if 'edited_transact_items' not in st.session_state or st.session_state.edited_transact_items.empty:
                    st.warning("No item data to submit.")
                    valid = False
                elif 'edited_transact_totals' not in st.session_state or st.session_state.edited_transact_totals.empty:
                    st.warning("No total data to submit.")
                    valid = False
                else:
                    gold_items_df = st.session_state.edited_transact_items.copy()
                    gold_totals_df = st.session_state.edited_transact_totals.copy()

            if valid:
                try:
                    st.write("Submitting...")
                    current_ts = datetime.now()

                    # Prepare Data for Gold Layer
                    # Items
                    gold_items_df['INVOICE_ID'] = selected_invoice_id
                    gold_items_df['REVIEWED_BY'] = CURRENT_USER
                    gold_items_df['REVIEWED_TIMESTAMP'] = current_ts
                    gold_items_df['NOTES'] = review_notes

                    # Insert items data
                    st.write(f"Writing {len(gold_items_df)} rows to {GOLD_ITEMS_TABLE}...")
                    
                    # Delete existing before insert
                    conn.query(f"DELETE FROM {GOLD_ITEMS_TABLE} WHERE INVOICE_ID = '{selected_invoice_id}'")
                    
                    # Insert new data (using a more SiS-compatible approach)
                    for _, row in gold_items_df.iterrows():
                        insert_query = f"""
                        INSERT INTO {GOLD_ITEMS_TABLE} 
                        (INVOICE_ID, PRODUCT_NAME, QUANTITY, UNIT_PRICE, TOTAL_PRICE, REVIEWED_BY, REVIEWED_TIMESTAMP, NOTES)
                        VALUES ('{row['INVOICE_ID']}', '{str(row['PRODUCT_NAME']).replace("'", "''")}', 
                               {row['QUANTITY']}, {row['UNIT_PRICE']}, {row['TOTAL_PRICE']}, 
                               '{CURRENT_USER}', '{current_ts.strftime('%Y-%m-%d %H:%M:%S')}', 
                               '{str(review_notes).replace("'", "''")}')
                        """
                        conn.query(insert_query)
                    
                    st.success(f"Successfully saved corrected items to {GOLD_ITEMS_TABLE}")

                    # Totals
                    gold_totals_df['INVOICE_ID'] = selected_invoice_id
                    gold_totals_df['REVIEWED_BY'] = CURRENT_USER
                    gold_totals_df['REVIEWED_TIMESTAMP'] = current_ts
                    gold_totals_df['NOTES'] = review_notes

                    st.write(f"Writing corrected totals to {GOLD_TOTALS_TABLE}...")
                    
                    # Delete existing before insert
                    conn.query(f"DELETE FROM {GOLD_TOTALS_TABLE} WHERE INVOICE_ID = '{selected_invoice_id}'")
                    
                    # Insert totals data
                    for _, row in gold_totals_df.iterrows():
                        insert_query = f"""
                        INSERT INTO {GOLD_TOTALS_TABLE} 
                        (INVOICE_ID, INVOICE_DATE, SUBTOTAL, TAX, TOTAL, REVIEWED_BY, REVIEWED_TIMESTAMP, NOTES)
                        VALUES ('{row['INVOICE_ID']}', '{row['INVOICE_DATE']}', 
                               {row['SUBTOTAL']}, {row['TAX']}, {row['TOTAL']}, 
                               '{CURRENT_USER}', '{current_ts.strftime('%Y-%m-%d %H:%M:%S')}', 
                               '{str(review_notes).replace("'", "''")}')
                        """
                        conn.query(insert_query)
                    
                    st.success(f"Successfully saved corrected totals to {GOLD_TOTALS_TABLE}")

                    # Update reconcile Tables Review Status
                    st.write("Updating review status in reconcile tables...")
                    
                    update_query = f"""
                    UPDATE {RECONCILE_ITEMS_TABLE}
                    SET REVIEW_STATUS = 'Reviewed',
                        REVIEWED_BY = '{CURRENT_USER}',
                        REVIEWED_TIMESTAMP = '{current_ts.strftime('%Y-%m-%d %H:%M:%S')}',
                        NOTES = '{str(review_notes).replace("'", "''")}'
                    WHERE INVOICE_ID = '{selected_invoice_id}'
                    """
                    conn.query(update_query)

                    update_query = f"""
                    UPDATE {RECONCILE_TOTALS_TABLE}
                    SET REVIEW_STATUS = 'Reviewed',
                        REVIEWED_BY = '{CURRENT_USER}',
                        REVIEWED_TIMESTAMP = '{current_ts.strftime('%Y-%m-%d %H:%M:%S')}',
                        NOTES = '{str(review_notes).replace("'", "''")}'
                    WHERE INVOICE_ID = '{selected_invoice_id}'
                    """
                    conn.query(update_query)

                    st.success(f"Successfully updated review status for invoice {selected_invoice_id} in reconcile tables.")

                    # Clear Cache and Rerun
                    st.cache_data.clear()
                    st.info("Refreshing application...")
                    st.rerun()

                except Exception as e:
                    st.error(f"An error occurred during submission: {e}")

    else:
        st.warning("Please select an invoice ID from the dropdown above.")

def show_ai_analysis():
    """AI analysis interface"""
    st.header("🔍 AI Invoice Analysis")
    
    # Get invoice list
    try:
        invoice_query = f"SELECT DISTINCT invoice_id FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS ORDER BY invoice_id"
        invoices = conn.query(invoice_query)['INVOICE_ID'].tolist()
        
        selected_invoice = st.selectbox("Select Invoice:", [""] + invoices)
        
        if selected_invoice:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🚨 Fraud Risk Analysis")
                if st.button("Analyze Fraud Risk"):
                    with st.spinner("AI analyzing..."):
                        analysis = ai_fraud_analysis(selected_invoice)
                    st.write(analysis)
            
            with col2:
                st.subheader("🏷️ Invoice Categorization")
                if st.button("Categorize Invoice"):
                    with st.spinner("AI categorizing..."):
                        category = ai_categorize_invoice(selected_invoice)
                    st.write(category)
                    
    except Exception as e:
        st.error(f"Error loading invoices: {e}")

def show_ai_assistant():
    """AI assistant chat interface"""
    st.header("💬 AI Assistant")
    st.write("Ask questions about your invoice data!")
    
    # Display chat history
    for i, (question, answer) in enumerate(st.session_state.chat_history):
        with st.chat_message("user"):
            st.write(question)
        with st.chat_message("assistant"):
            st.write(answer)
    
    # Chat input
    if prompt := st.chat_input("Ask about your invoices..."):
        # Add user message to chat history
        with st.chat_message("user"):
            st.write(prompt)
        
        # Get AI response
        with st.chat_message("assistant"):
            with st.spinner("AI thinking..."):
                response = ai_assistant_query(prompt)
            st.write(response)
        
        # Save to chat history
        st.session_state.chat_history.append((prompt, response))

def show_analytics():
    """Show analytics dashboard"""
    st.header("📊 Analytics Dashboard")
    
    try:
        # Monthly spend trends
        monthly_query = f"""
        SELECT 
            DATE_TRUNC('month', invoice_date) as month,
            SUM(total) as monthly_total,
            COUNT(*) as invoice_count
        FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
        WHERE invoice_date IS NOT NULL
        GROUP BY DATE_TRUNC('month', invoice_date)
        ORDER BY month
        """
        monthly_data = conn.query(monthly_query)
        
        if not monthly_data.empty:
            fig = px.line(monthly_data, x='MONTH', y='MONTHLY_TOTAL', 
                         title='Monthly Spend Trends',
                         labels={'MONTHLY_TOTAL': 'Total Amount ($)', 'MONTH': 'Month'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Top spending categories (if you have category data)
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Invoice Distribution")
            amount_ranges = conn.query(f"""
                SELECT 
                    CASE 
                        WHEN total < 100 THEN 'Under $100'
                        WHEN total < 500 THEN '$100-$500'
                        WHEN total < 1000 THEN '$500-$1000'
                        ELSE 'Over $1000'
                    END as amount_range,
                    COUNT(*) as count
                FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
                GROUP BY amount_range
                ORDER BY count DESC
            """)
            
            if not amount_ranges.empty:
                fig = px.pie(amount_ranges, values='COUNT', names='AMOUNT_RANGE', 
                           title='Invoice Amount Distribution')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🎯 Key Metrics")
            metrics = get_invoice_metrics()
            if metrics:
                st.metric("Total Invoices", f"{metrics.get('TOTAL_INVOICES', 0):,}")
                st.metric("Total Value", f"${metrics.get('TOTAL_AMOUNT', 0):,.2f}")
                st.metric("Average Invoice", f"${metrics.get('AVG_AMOUNT', 0):,.2f}")
                
                if metrics.get('EARLIEST_DATE') and metrics.get('LATEST_DATE'):
                    date_range = pd.to_datetime(metrics['LATEST_DATE']) - pd.to_datetime(metrics['EARLIEST_DATE'])
                    st.metric("Date Range", f"{date_range.days} days")
        
    except Exception as e:
        st.error(f"Error loading analytics: {e}")

# --- Main Application ---

# Enhanced CSS styling
st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #667eea;
    }
    .ai-feature {
        background: linear-gradient(45deg, #f093fb 0%, #f5576c 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)

# Enhanced header
st.markdown("""
    <div class="main-header">
        <h1>🚀 Enhanced Invoice Intelligence Platform</h1>
        <p>Powered by Snowflake Cortex AI + Document AI + Advanced Analytics</p>
        <p><em>Running in Streamlit in Snowflake (SiS)</em></p>
    </div>
    """, unsafe_allow_html=True)

# Sidebar navigation
with st.sidebar:
    st.markdown("### 🎛️ Navigation")
    page = st.selectbox("Choose Module:", [
        "🏠 Dashboard",
        "🔍 Invoice Reconciliation",
        "🧠 AI Analysis", 
        "💬 AI Assistant",
        "📊 Analytics"
    ])
    
    st.divider()
    st.markdown("### ℹ️ System Info")
    st.success("✅ Streamlit in Snowflake")
    st.success("✅ Document AI Active")
    try:
        conn.query("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', 'test')")
        st.success("✅ Cortex AI Ready")
    except:
        st.warning("⚠️ Cortex AI Limited")

# Main content
if page == "🏠 Dashboard":
    show_dashboard()
elif page == "🔍 Invoice Reconciliation":
    show_reconciliation()
elif page == "🧠 AI Analysis":
    show_ai_analysis()
elif page == "💬 AI Assistant":
    show_ai_assistant()
elif page == "📊 Analytics":
    show_analytics()

# Footer
st.divider()
st.markdown("**🚀 Powered by Snowflake Cortex AI + Document AI**") 