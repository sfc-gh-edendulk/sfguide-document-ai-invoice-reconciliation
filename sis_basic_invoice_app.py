import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(
    page_title="🚀 AI Invoice Intelligence - SiS", 
    layout="wide"
)

# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# --- Snowflake Connection ---
@st.cache_resource
def get_connection():
    return st.connection("snowflake")

conn = get_connection()

# --- Main Header ---
st.markdown("""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            padding: 2rem; border-radius: 15px; margin-bottom: 2rem; 
            color: white; text-align: center;">
    <h1>🚀 AI-Powered Invoice Intelligence</h1>
    <p>Enhanced with Snowflake Cortex AI • Running in Streamlit in Snowflake</p>
</div>
""", unsafe_allow_html=True)

# --- Core Functions ---

def get_invoice_metrics():
    """Get basic invoice metrics"""
    try:
        query = """
        SELECT 
            COUNT(DISTINCT invoice_id) as total_invoices,
            SUM(total) as total_amount,
            AVG(total) as avg_amount,
            MIN(invoice_date) as earliest_date,
            MAX(invoice_date) as latest_date
        FROM DOC_AI_QS_DB.DOC_AI_SCHEMA.TRANSACT_TOTALS
        """
        return conn.query(query).iloc[0]
    except Exception as e:
        st.error(f"Error loading metrics: {e}")
        return None

def ai_fraud_analysis(invoice_id):
    """Simple AI fraud analysis"""
    try:
        # Get invoice data
        query = f"""
        SELECT 
            t.invoice_id,
            t.total,
            t.invoice_date,
            COUNT(i.product_name) as item_count,
            (SELECT AVG(total) FROM DOC_AI_QS_DB.DOC_AI_SCHEMA.TRANSACT_TOTALS) as avg_total
        FROM DOC_AI_QS_DB.DOC_AI_SCHEMA.TRANSACT_TOTALS t
        LEFT JOIN DOC_AI_QS_DB.DOC_AI_SCHEMA.TRANSACT_ITEMS i ON t.invoice_id = i.invoice_id
        WHERE t.invoice_id = '{invoice_id}'
        GROUP BY t.invoice_id, t.total, t.invoice_date
        """
        
        data = conn.query(query)
        if data.empty:
            return "No data found for analysis"
        
        row = data.iloc[0]
        
        # Create AI prompt
        prompt = f"""
        Analyze this invoice for potential fraud risks:
        - Invoice ID: {row['INVOICE_ID']}
        - Amount: ${row['TOTAL']:,.2f}
        - Date: {row['INVOICE_DATE']}
        - Items: {row['ITEM_COUNT']}
        - Average invoice amount: ${row['AVG_TOTAL']:,.2f}
        
        Provide a risk assessment (Low/Medium/High) and brief explanation.
        """
        
        # Use Cortex Complete
        ai_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', $${prompt}$$) as analysis"
        result = conn.query(ai_query)
        
        return result.iloc[0]['ANALYSIS'] if not result.empty else "AI analysis unavailable"
        
    except Exception as e:
        return f"Error in analysis: {e}"

def ai_categorize_invoice(invoice_id):
    """AI invoice categorization"""
    try:
        # Get product data
        query = f"""
        SELECT 
            LISTAGG(product_name, ', ') as products,
            SUM(total_price) as total_amount
        FROM DOC_AI_QS_DB.DOC_AI_SCHEMA.TRANSACT_ITEMS
        WHERE invoice_id = '{invoice_id}'
        GROUP BY invoice_id
        """
        
        data = conn.query(query)
        if data.empty:
            return "No product data found"
        
        row = data.iloc[0]
        
        prompt = f"""
        Categorize this invoice based on products:
        Products: {row['PRODUCTS']}
        Amount: ${row['TOTAL_AMOUNT']:,.2f}
        
        Choose from: Food & Beverages, Office Supplies, Technology, Services, Other
        Provide category and confidence level.
        """
        
        ai_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', $${prompt}$$) as category"
        result = conn.query(ai_query)
        
        return result.iloc[0]['CATEGORY'] if not result.empty else "Categorization unavailable"
        
    except Exception as e:
        return f"Error in categorization: {e}"

def ai_chatbot(question):
    """Simple AI chatbot"""
    try:
        # Get context
        metrics = get_invoice_metrics()
        
        context = f"""
        You are an AI assistant for an invoice management system.
        System has {metrics['TOTAL_INVOICES']} invoices worth ${metrics['TOTAL_AMOUNT']:,.2f}
        Date range: {metrics['EARLIEST_DATE']} to {metrics['LATEST_DATE']}
        
        Question: {question}
        
        Provide a helpful response about invoice data and analysis.
        """
        
        ai_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', $${context}$$) as response"
        result = conn.query(ai_query)
        
        return result.iloc[0]['RESPONSE'] if not result.empty else "Sorry, I couldn't process your question."
        
    except Exception as e:
        return f"Error: {e}"

# --- UI Components ---

def show_dashboard():
    """Main dashboard"""
    st.header("📊 Invoice Dashboard")
    
    metrics = get_invoice_metrics()
    if metrics is not None:
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric("📋 Total Invoices", f"{metrics['TOTAL_INVOICES']:,}")
        col2.metric("💰 Total Value", f"${metrics['TOTAL_AMOUNT']:,.2f}")
        col3.metric("📈 Average Invoice", f"${metrics['AVG_AMOUNT']:,.2f}")
        col4.metric("📅 Date Range", f"{(metrics['LATEST_DATE'] - metrics['EARLIEST_DATE']).days} days")

def show_ai_analysis():
    """AI analysis interface"""
    st.header("🔍 AI Invoice Analysis")
    
    # Get invoice list
    try:
        invoice_query = "SELECT DISTINCT invoice_id FROM DOC_AI_QS_DB.DOC_AI_SCHEMA.TRANSACT_TOTALS ORDER BY invoice_id"
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
    """AI assistant chatbot"""
    st.header("💬 AI Assistant")
    
    # Show chat history
    if st.session_state.chat_history:
        for role, message in st.session_state.chat_history[-5:]:
            if role == "user":
                st.markdown(f"**You:** {message}")
            else:
                st.markdown(f"**AI:** {message}")
    
    # Chat input
    user_input = st.text_input("Ask about your invoices:", placeholder="e.g., What are my spending patterns?")
    
    if st.button("Ask AI") and user_input:
        st.session_state.chat_history.append(("user", user_input))
        
        with st.spinner("AI thinking..."):
            response = ai_chatbot(user_input)
        
        st.session_state.chat_history.append(("assistant", response))
        st.markdown(f"**AI:** {response}")

def show_analytics():
    """Simple analytics"""
    st.header("📈 Spend Analytics")
    
    try:
        # Monthly trends
        trend_query = """
        SELECT 
            DATE_TRUNC('month', invoice_date) as month,
            COUNT(*) as invoice_count,
            SUM(total) as total_spend
        FROM DOC_AI_QS_DB.DOC_AI_SCHEMA.TRANSACT_TOTALS
        GROUP BY month
        ORDER BY month
        """
        
        trends = conn.query(trend_query)
        
        if not trends.empty:
            fig = px.line(trends, x='MONTH', y='TOTAL_SPEND', 
                         title='Monthly Spend Trends',
                         markers=True)
            st.plotly_chart(fig, use_container_width=True)
            
            # Category analysis
            category_query = """
            SELECT 
                CASE 
                    WHEN LOWER(product_name) LIKE '%bread%' OR LOWER(product_name) LIKE '%milk%' THEN 'Food & Beverages'
                    WHEN LOWER(product_name) LIKE '%chicken%' OR LOWER(product_name) LIKE '%cheese%' THEN 'Protein'
                    ELSE 'Other'
                END as category,
                SUM(total_price) as spend
            FROM DOC_AI_QS_DB.DOC_AI_SCHEMA.TRANSACT_ITEMS
            GROUP BY category
            """
            
            categories = conn.query(category_query)
            
            if not categories.empty:
                fig2 = px.pie(categories, values='SPEND', names='CATEGORY', 
                             title='Spend by Category')
                st.plotly_chart(fig2, use_container_width=True)
                
    except Exception as e:
        st.error(f"Error loading analytics: {e}")

# --- Main Application ---

# Sidebar navigation
with st.sidebar:
    st.markdown("### 🎛️ Navigation")
    page = st.selectbox("Choose Module:", [
        "🏠 Dashboard",
        "🔍 AI Analysis", 
        "💬 AI Assistant",
        "📊 Analytics"
    ])
    
    st.divider()
    st.markdown("### ℹ️ System Info")
    st.success("✅ Streamlit in Snowflake")
    st.success("✅ Cortex AI Enabled")

# Main content
if page == "🏠 Dashboard":
    show_dashboard()
elif page == "🔍 AI Analysis":
    show_ai_analysis()
elif page == "💬 AI Assistant":
    show_ai_assistant()
elif page == "📊 Analytics":
    show_analytics()

# Footer
st.divider()
st.markdown("**🚀 Powered by Snowflake Cortex AI + Document AI**") 