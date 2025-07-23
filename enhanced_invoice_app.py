import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, current_timestamp, sql_expr
import snowflake.snowpark as snowpark
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pypdfium2 as pdfium
import io
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import base64

st.set_page_config(
    page_title="🚀 AI-Powered Invoice Intelligence", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuration ---
DB_NAME = "DOC_AI_QS_DB"
SCHEMA_NAME = "DOC_AI_SCHEMA"
STAGE_NAME = "DOC_AI_STAGE"

RECONCILE_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_ITEMS"
RECONCILE_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_TOTALS"
BRONZE_TRANSACT_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.TRANSACT_ITEMS"
BRONZE_TRANSACT_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS"
BRONZE_DOCAI_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOCAI_INVOICE_ITEMS"
BRONZE_DOCAI_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOCAI_INVOICE_TOTALS"
GOLD_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.GOLD_INVOICE_ITEMS"
GOLD_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.GOLD_INVOICE_TOTALS"

# Initialize session state
if 'processed_invoice_id' not in st.session_state:
    st.session_state.processed_invoice_id = None
if 'cached_mismatch_summary' not in st.session_state:
    st.session_state.cached_mismatch_summary = None
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# --- Get Snowflake Session ---
try:
    session = get_active_session()
    st.success("❄️ Snowflake session established!")
    CURRENT_USER = session.get_current_role().replace("\"", "")
except Exception as e:
    st.error(f"Error getting Snowflake session: {e}")
    st.stop()

# --- Enhanced AI Functions ---

def analyze_invoice_fraud_risk(session, invoice_id):
    """Uses Cortex AI to analyze fraud risk patterns in invoice data"""
    try:
        # Get invoice data for analysis
        query = f"""
        SELECT 
            t.invoice_id,
            t.total,
            t.invoice_date,
            COUNT(i.product_name) as item_count,
            AVG(i.unit_price) as avg_unit_price,
            MAX(i.unit_price) as max_unit_price,
            MIN(i.unit_price) as min_unit_price,
            STDDEV(i.unit_price) as price_variance
        FROM {BRONZE_TRANSACT_TOTALS_TABLE} t
        LEFT JOIN {BRONZE_TRANSACT_ITEMS_TABLE} i ON t.invoice_id = i.invoice_id
        WHERE t.invoice_id = '{invoice_id}'
        GROUP BY t.invoice_id, t.total, t.invoice_date
        """
        
        invoice_data = session.sql(query).to_pandas()
        
        if invoice_data.empty:
            return "No data found for fraud analysis"
        
        row = invoice_data.iloc[0]
        
        # Create analysis prompt
        prompt = f"""
        Analyze this invoice for potential fraud risk factors:
        
        Invoice ID: {row['INVOICE_ID']}
        Total Amount: ${row['TOTAL']}
        Date: {row['INVOICE_DATE']}
        Number of Items: {row['ITEM_COUNT']}
        Average Unit Price: ${row['AVG_UNIT_PRICE']:.2f}
        Price Range: ${row['MIN_UNIT_PRICE']:.2f} - ${row['MAX_UNIT_PRICE']:.2f}
        Price Variance: {row['PRICE_VARIANCE']:.2f}
        
        Provide a fraud risk assessment (Low/Medium/High) and explain key risk factors.
        Consider unusual pricing patterns, suspicious amounts, or anomalous item counts.
        """
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as analysis").collect()
        return response_df[0]["ANALYSIS"] if response_df else "Could not complete fraud analysis"
        
    except Exception as e:
        return f"Error in fraud analysis: {e}"

def categorize_invoice_with_ai(session, invoice_id):
    """Uses Cortex AI to categorize invoices by type and vendor"""
    try:
        # Get invoice items for categorization
        query = f"""
        SELECT 
            invoice_id,
            LISTAGG(product_name, ', ') as products,
            SUM(total_price) as total_amount
        FROM {BRONZE_TRANSACT_ITEMS_TABLE}
        WHERE invoice_id = '{invoice_id}'
        GROUP BY invoice_id
        """
        
        invoice_data = session.sql(query).to_pandas()
        
        if invoice_data.empty:
            return {"category": "Unknown", "confidence": 0, "reasoning": "No data available"}
        
        products = invoice_data.iloc[0]['PRODUCTS']
        total = invoice_data.iloc[0]['TOTAL_AMOUNT']
        
        prompt = f"""
        Categorize this invoice based on the products and amount:
        
        Products: {products}
        Total Amount: ${total}
        
        Choose the most appropriate category:
        - Office Supplies
        - Food & Beverages  
        - Technology
        - Services
        - Travel & Entertainment
        - Manufacturing Supplies
        - Other
        
        Respond in JSON format with: category, confidence (0-1), reasoning
        """
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as categorization").collect()
        
        if response_df:
            try:
                return json.loads(response_df[0]["CATEGORIZATION"])
            except:
                return {"category": "Other", "confidence": 0.5, "reasoning": "Could not parse AI response"}
        
        return {"category": "Unknown", "confidence": 0, "reasoning": "No AI response"}
        
    except Exception as e:
        return {"category": "Error", "confidence": 0, "reasoning": f"Error: {e}"}

def ai_chatbot_query(session, user_question, context_data=None):
    """AI chatbot for natural language queries about invoice data"""
    try:
        # Get relevant context data if not provided
        if context_data is None:
            context_query = f"""
            SELECT 
                COUNT(*) as total_invoices,
                SUM(total) as total_amount,
                AVG(total) as avg_amount,
                MIN(invoice_date) as earliest_date,
                MAX(invoice_date) as latest_date
            FROM {BRONZE_TRANSACT_TOTALS_TABLE}
            """
            context_data = session.sql(context_query).to_pandas().iloc[0]
        
        # Create enriched prompt with context
        prompt = f"""
        You are an AI assistant for an invoice management system. Answer the user's question based on the following context:
        
        System Context:
        - Total Invoices: {context_data.get('TOTAL_INVOICES', 'N/A')}
        - Total Amount: ${context_data.get('TOTAL_AMOUNT', 0):,.2f}
        - Average Amount: ${context_data.get('AVG_AMOUNT', 0):,.2f}
        - Date Range: {context_data.get('EARLIEST_DATE', 'N/A')} to {context_data.get('LATEST_DATE', 'N/A')}
        
        Available tables: TRANSACT_ITEMS, TRANSACT_TOTALS, DOCAI_INVOICE_ITEMS, DOCAI_INVOICE_TOTALS, GOLD_INVOICE_ITEMS, GOLD_INVOICE_TOTALS
        
        User Question: {user_question}
        
        Provide a helpful response. If the question requires specific data queries, suggest what analysis could be performed.
        """
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as response").collect()
        return response_df[0]["RESPONSE"] if response_df else "I'm sorry, I couldn't process your question."
        
    except Exception as e:
        return f"Error processing your question: {e}"

def generate_spend_insights(session):
    """Generate AI-powered spend analytics insights"""
    try:
        # Get spend analytics data
        query = f"""
        WITH monthly_spend AS (
            SELECT 
                DATE_TRUNC('month', invoice_date) as month,
                COUNT(*) as invoice_count,
                SUM(total) as total_spend,
                AVG(total) as avg_invoice_amount
            FROM {BRONZE_TRANSACT_TOTALS_TABLE}
            GROUP BY DATE_TRUNC('month', invoice_date)
            ORDER BY month
        ),
        top_products AS (
            SELECT 
                product_name,
                COUNT(*) as frequency,
                SUM(total_price) as total_spent,
                AVG(unit_price) as avg_price
            FROM {BRONZE_TRANSACT_ITEMS_TABLE}
            GROUP BY product_name
            ORDER BY total_spent DESC
            LIMIT 10
        )
        SELECT 
            (SELECT LISTAGG(month || ': $' || total_spend, ', ') FROM monthly_spend) as monthly_data,
            (SELECT LISTAGG(product_name || ' ($' || total_spent || ')', ', ') FROM top_products) as top_products_data
        """
        
        data = session.sql(query).to_pandas().iloc[0]
        
        prompt = f"""
        Analyze this spend data and provide 3-5 key insights:
        
        Monthly Spend Data: {data['MONTHLY_DATA']}
        Top Products by Spend: {data['TOP_PRODUCTS_DATA']}
        
        Provide insights about:
        - Spending trends
        - Cost optimization opportunities  
        - Unusual patterns
        - Recommendations
        
        Keep insights business-focused and actionable.
        """
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as insights").collect()
        return response_df[0]["INSIGHTS"] if response_df else "Could not generate insights"
        
    except Exception as e:
        return f"Error generating insights: {e}"

def detect_anomalies_with_ai(session):
    """Use Cortex AI to detect anomalies in invoice patterns"""
    try:
        query = f"""
        WITH invoice_stats AS (
            SELECT 
                invoice_id,
                total,
                invoice_date,
                (SELECT AVG(total) FROM {BRONZE_TRANSACT_TOTALS_TABLE}) as avg_total,
                (SELECT STDDEV(total) FROM {BRONZE_TRANSACT_TOTALS_TABLE}) as stddev_total
            FROM {BRONZE_TRANSACT_TOTALS_TABLE}
        )
        SELECT 
            invoice_id,
            total,
            invoice_date,
            CASE 
                WHEN ABS(total - avg_total) > 2 * stddev_total THEN 'OUTLIER'
                ELSE 'NORMAL'
            END as statistical_flag,
            ((total - avg_total) / stddev_total) as z_score
        FROM invoice_stats
        WHERE statistical_flag = 'OUTLIER'
        ORDER BY ABS(z_score) DESC
        LIMIT 5
        """
        
        anomalies = session.sql(query).to_pandas()
        
        if anomalies.empty:
            return "No statistical anomalies detected in invoice amounts."
        
        anomaly_list = ""
        for _, row in anomalies.iterrows():
            anomaly_list += f"Invoice {row['INVOICE_ID']}: ${row['TOTAL']} (Z-score: {row['Z_SCORE']:.2f}), "
        
        prompt = f"""
        Analyze these statistical anomalies in invoice data:
        
        {anomaly_list}
        
        Provide:
        1. Assessment of each anomaly's significance
        2. Possible explanations for unusual amounts
        3. Recommendations for investigation
        
        Consider business context - some high amounts may be legitimate.
        """
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as analysis").collect()
        return response_df[0]["ANALYSIS"] if response_df else "Could not analyze anomalies"
        
    except Exception as e:
        return f"Error detecting anomalies: {e}"

# --- Enhanced Visualization Functions ---

def create_spend_analytics_dashboard(session):
    """Create comprehensive spend analytics visualizations"""
    try:
        # Monthly spend trend
        monthly_query = f"""
        SELECT 
            DATE_TRUNC('month', invoice_date) as month,
            COUNT(*) as invoice_count,
            SUM(total) as total_spend,
            AVG(total) as avg_invoice_amount
        FROM {BRONZE_TRANSACT_TOTALS_TABLE}
        GROUP BY month
        ORDER BY month
        """
        monthly_data = session.sql(monthly_query).to_pandas()
        
        # Category analysis (using AI categorization)
        category_query = f"""
        SELECT 
            CASE 
                WHEN product_name LIKE '%Bread%' OR product_name LIKE '%Yogurt%' 
                     OR product_name LIKE '%Milk%' OR product_name LIKE '%Eggs%' THEN 'Food & Beverages'
                WHEN product_name LIKE '%Rice%' OR product_name LIKE '%Onions%' 
                     OR product_name LIKE '%Tomatoes%' THEN 'Groceries'
                WHEN product_name LIKE '%Chicken%' OR product_name LIKE '%Cheese%' THEN 'Protein'
                ELSE 'Other'
            END as category,
            COUNT(*) as item_count,
            SUM(total_price) as category_spend
        FROM {BRONZE_TRANSACT_ITEMS_TABLE}
        GROUP BY category
        ORDER BY category_spend DESC
        """
        category_data = session.sql(category_query).to_pandas()
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Monthly Spend Trend', 'Spend by Category', 'Invoice Volume', 'Average Invoice Amount'),
            specs=[[{"secondary_y": False}, {"type": "pie"}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Monthly spend trend
        fig.add_trace(
            go.Scatter(x=monthly_data['MONTH'], y=monthly_data['TOTAL_SPEND'],
                      mode='lines+markers', name='Total Spend', line=dict(color='#1f77b4')),
            row=1, col=1
        )
        
        # Category pie chart
        fig.add_trace(
            go.Pie(labels=category_data['CATEGORY'], values=category_data['CATEGORY_SPEND'],
                   name="Category Spend"),
            row=1, col=2
        )
        
        # Invoice volume
        fig.add_trace(
            go.Bar(x=monthly_data['MONTH'], y=monthly_data['INVOICE_COUNT'],
                   name='Invoice Count', marker_color='#ff7f0e'),
            row=2, col=1
        )
        
        # Average invoice amount
        fig.add_trace(
            go.Scatter(x=monthly_data['MONTH'], y=monthly_data['AVG_INVOICE_AMOUNT'],
                      mode='lines+markers', name='Avg Amount', line=dict(color='#2ca02c')),
            row=2, col=2
        )
        
        fig.update_layout(height=600, showlegend=True, title_text="📊 Spend Analytics Dashboard")
        
        return fig, monthly_data, category_data
        
    except Exception as e:
        st.error(f"Error creating dashboard: {e}")
        return None, None, None

def create_reconciliation_metrics_viz(session):
    """Create visualization for reconciliation metrics"""
    try:
        metrics_query = f"""
        WITH reconcile_status AS (
            SELECT 
                'Items' as table_type,
                review_status,
                COUNT(*) as count
            FROM {RECONCILE_ITEMS_TABLE}
            GROUP BY review_status
            UNION ALL
            SELECT 
                'Totals' as table_type,
                review_status,
                COUNT(*) as count
            FROM {RECONCILE_TOTALS_TABLE}
            GROUP BY review_status
        )
        SELECT * FROM reconcile_status
        """
        
        metrics_data = session.sql(metrics_query).to_pandas()
        
        fig = px.bar(metrics_data, x='TABLE_TYPE', y='COUNT', color='REVIEW_STATUS',
                     title='📋 Reconciliation Status Overview',
                     labels={'COUNT': 'Number of Invoices', 'TABLE_TYPE': 'Data Type'})
        
        fig.update_layout(height=400)
        return fig
        
    except Exception as e:
        st.error(f"Error creating reconciliation metrics: {e}")
        return None

# --- Enhanced UI Components ---

def render_ai_chatbot():
    """Render AI chatbot interface"""
    st.subheader("🤖 AI Invoice Assistant")
    
    # Chat history display
    chat_container = st.container()
    with chat_container:
        for i, (role, message) in enumerate(st.session_state.chat_history[-10:]):  # Show last 10 messages
            if role == "user":
                st.markdown(f"**You:** {message}")
            else:
                st.markdown(f"**AI Assistant:** {message}")
    
    # Chat input
    user_input = st.text_input("Ask me about your invoices...", 
                              placeholder="e.g., 'What are my highest spending categories?' or 'Show me any unusual invoices'")
    
    if st.button("Send") and user_input:
        # Add user message to history
        st.session_state.chat_history.append(("user", user_input))
        
        # Get AI response
        with st.spinner("AI is thinking..."):
            ai_response = ai_chatbot_query(session, user_input)
        
        # Add AI response to history
        st.session_state.chat_history.append(("assistant", ai_response))
        
        # Rerun to show new messages
        st.rerun()

def render_fraud_detection_panel(selected_invoice_id):
    """Render fraud detection analysis panel"""
    if selected_invoice_id:
        st.subheader("🔍 AI Fraud Risk Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🚨 Analyze Fraud Risk"):
                with st.spinner("Analyzing fraud patterns..."):
                    fraud_analysis = analyze_invoice_fraud_risk(session, selected_invoice_id)
                st.write(fraud_analysis)
        
        with col2:
            if st.button("🏷️ AI Categorization"):
                with st.spinner("Categorizing invoice..."):
                    category_info = categorize_invoice_with_ai(session, selected_invoice_id)
                
                st.json(category_info)

def render_analytics_insights():
    """Render advanced analytics and insights"""
    st.header("📊 AI-Powered Analytics & Insights")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Spend Dashboard", "🔍 Anomaly Detection", "💡 AI Insights", "📋 Reconciliation Status"])
    
    with tab1:
        st.subheader("Spend Analytics Dashboard")
        dashboard_fig, monthly_data, category_data = create_spend_analytics_dashboard(session)
        
        if dashboard_fig:
            st.plotly_chart(dashboard_fig, use_container_width=True)
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            if not monthly_data.empty:
                total_spend = monthly_data['TOTAL_SPEND'].sum()
                total_invoices = monthly_data['INVOICE_COUNT'].sum()
                avg_monthly_spend = monthly_data['TOTAL_SPEND'].mean()
                
                col1.metric("Total Spend", f"${total_spend:,.2f}")
                col2.metric("Total Invoices", f"{total_invoices:,}")
                col3.metric("Avg Monthly Spend", f"${avg_monthly_spend:,.2f}")
                col4.metric("Categories", len(category_data) if not category_data.empty else 0)
    
    with tab2:
        st.subheader("🚨 Anomaly Detection")
        
        if st.button("🔍 Detect Anomalies with AI"):
            with st.spinner("AI is analyzing patterns..."):
                anomaly_analysis = detect_anomalies_with_ai(session)
            
            st.write(anomaly_analysis)
    
    with tab3:
        st.subheader("💡 AI-Generated Insights")
        
        if st.button("🧠 Generate Spend Insights"):
            with st.spinner("AI is generating insights..."):
                insights = generate_spend_insights(session)
            
            st.write(insights)
    
    with tab4:
        st.subheader("📋 Reconciliation Metrics")
        
        reconcile_fig = create_reconciliation_metrics_viz(session)
        if reconcile_fig:
            st.plotly_chart(reconcile_fig, use_container_width=True)

# --- Main Application ---

def main():
    # Custom CSS for enhanced styling
    st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #667eea;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .success-message {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        padding: 0.75rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Enhanced header
    st.markdown("""
    <div class="main-header">
        <h1 style="color: white; margin: 0;">🚀 AI-Powered Invoice Intelligence Platform</h1>
        <p style="color: white; margin: 0; opacity: 0.9;">Advanced Document AI + Snowflake Cortex + Analytics</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown(f"**Connected as:** `{CURRENT_USER}` | **Session:** Active ❄️")
    
    # Sidebar navigation
    with st.sidebar:
        st.header("🎛️ Navigation")
        
        page = st.selectbox("Choose Module:", [
            "📊 Analytics Dashboard", 
            "🔍 Invoice Review & Reconciliation",
            "🤖 AI Assistant", 
            "📈 Advanced Analytics",
            "⚙️ System Management"
        ])
        
        st.divider()
        
        # File upload section
        st.header("📂 Upload Documents")
        uploaded = st.file_uploader("PDF / Image", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True)
        
        if uploaded:
            for uploaded_file in uploaded:
                file_bytes = uploaded_file.read()
                file_name = uploaded_file.name
                
                session.file.put_stream(io.BytesIO(file_bytes), f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{file_name}", overwrite=True, auto_compress=False)
                presigned_url = session.sql(f"SELECT GET_PRESIGNED_URL('@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}', '{file_name}', 360) AS URL").to_pandas().at[0, "URL"]
                session.sql(f"ALTER STAGE {DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME} REFRESH").collect()
                
                st.success(f"✅ Uploaded: {file_name}")
    
    # Route to different pages based on selection
    if page == "📊 Analytics Dashboard":
        render_analytics_insights()
    
    elif page == "🤖 AI Assistant":
        render_ai_chatbot()
    
    elif page == "🔍 Invoice Review & Reconciliation":
        # Enhanced version of original reconciliation interface
        st.header("🔍 Enhanced Invoice Review")
        
        # Load reconciliation data
        review_status_options = ['Pending Review', 'Reviewed', 'Auto-reconciled', 'All']
        selected_status = st.selectbox("Filter by Status:", review_status_options, index=0)
        
        # Your existing reconciliation logic here...
        # (I'll keep the original functionality but enhance it)
        from docai_invoice_qs_app import load_reconcile_data, load_bronze_data, get_invoice_reconciliation_metrics
        
        # Display metrics with enhanced visualization
        with st.spinner("Loading metrics..."):
            reconciliation_data = get_invoice_reconciliation_metrics(session)
        
        if reconciliation_data:
            col1, col2, col3, col4 = st.columns(4)
            
            col1.metric(
                "📋 Total Invoices", 
                f"{reconciliation_data['total_invoice_count']:,}",
                help="Total invoices in system"
            )
            col2.metric(
                "✅ Auto-Reconciled", 
                f"{reconciliation_data['count_auto_reconciled']:,}",
                delta=f"{(reconciliation_data['count_auto_reconciled']/reconciliation_data['total_invoice_count']*100):.1f}%"
            )
            col3.metric(
                "💰 Total Value", 
                f"${reconciliation_data['grand_total_amount']:,.2f}",
                help="Total value of all invoices"
            )
            col4.metric(
                "🎯 Reconciled Value", 
                f"${reconciliation_data['total_reconciled_amount']:,.2f}",
                delta=f"{reconciliation_data['reconciled_amount_ratio']:.1%}"
            )
        
        # Load and display invoices for review
        invoices_to_review_df, reconcile_items_details_df, reconcile_totals_details_df = load_reconcile_data(selected_status)
        
        if not invoices_to_review_df.empty:
            invoice_list = [""] + invoices_to_review_df['INVOICE_ID'].unique().tolist()
            selected_invoice_id = st.selectbox("Select Invoice:", invoice_list, index=0)
            
            if selected_invoice_id:
                # Enhanced invoice review with AI features
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Original invoice data display
                    bronze_data_dict = load_bronze_data(selected_invoice_id)
                    # ... (your existing display logic)
                
                with col2:
                    # AI enhancement panel
                    render_fraud_detection_panel(selected_invoice_id)
    
    elif page == "📈 Advanced Analytics":
        st.header("📈 Advanced Analytics Suite")
        
        # Your enhanced analytics go here
        render_analytics_insights()
    
    elif page == "⚙️ System Management":
        st.header("⚙️ System Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔄 Task Management")
            if st.button("▶️ Resume Reconciliation Task"):
                session.sql("ALTER TASK DOC_AI_QS_DB.DOC_AI_SCHEMA.RECONCILE RESUME").collect()
                st.success("Reconciliation task resumed")
            
            if st.button("⏸️ Suspend Reconciliation Task"):
                session.sql("ALTER TASK DOC_AI_QS_DB.DOC_AI_SCHEMA.RECONCILE SUSPEND").collect()
                st.success("Reconciliation task suspended")
        
        with col2:
            st.subheader("📊 System Health")
            try:
                task_status = session.sql("SHOW TASKS LIKE 'RECONCILE'").to_pandas()
                if not task_status.empty:
                    st.write(f"**Task State:** {task_status.iloc[0]['state']}")
            except:
                st.warning("Could not retrieve task status")

if __name__ == "__main__":
    main() 