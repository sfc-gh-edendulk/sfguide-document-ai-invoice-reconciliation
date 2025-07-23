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

# Initialize session state
session_state_keys = [
    'processed_invoice_id', 'cached_mismatch_summary', 'chat_history',
    'ai_insights_cache'
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

# --- Enhanced AI Functions for SiS ---

def ai_fraud_risk_analysis(invoice_id):
    """Enhanced fraud risk analysis using Cortex AI with statistical context"""
    try:
        # Get comprehensive invoice data with statistical context
        query = f"""
        WITH invoice_stats AS (
            SELECT 
                t.invoice_id,
                t.total,
                t.invoice_date,
                COUNT(i.product_name) as item_count,
                AVG(i.unit_price) as avg_unit_price,
                MAX(i.unit_price) as max_unit_price,
                STDDEV(i.unit_price) as price_stddev,
                -- Statistical context
                (SELECT AVG(total) FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS) as overall_avg,
                (SELECT STDDEV(total) FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS) as overall_stddev,
                DAYOFWEEK(t.invoice_date) as day_of_week
            FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS t
            LEFT JOIN {DB_NAME}.{SCHEMA_NAME}.TRANSACT_ITEMS i ON t.invoice_id = i.invoice_id
            WHERE t.invoice_id = '{invoice_id}'
            GROUP BY t.invoice_id, t.total, t.invoice_date
        )
        SELECT *,
               (total - overall_avg) / overall_stddev as z_score,
               CASE WHEN day_of_week IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END as day_type
        FROM invoice_stats
        """
        
        data = conn.query(query)
        
        if data.empty:
            return {"risk_level": "Unknown", "analysis": "No data found for analysis", "risk_score": 0}
        
        row = data.iloc[0]
        z_score = abs(row['Z_SCORE']) if pd.notna(row['Z_SCORE']) else 0
        
        # Enhanced AI prompt with more context
        prompt = f"""
        Perform a comprehensive fraud risk analysis for this invoice:
        
        INVOICE DETAILS:
        - ID: {row['INVOICE_ID']}
        - Amount: ${row['TOTAL']:,.2f}
        - Date: {row['INVOICE_DATE']} ({row['DAY_TYPE']})
        - Items Count: {row['ITEM_COUNT']}
        - Average Item Price: ${row['AVG_UNIT_PRICE']:.2f}
        - Price Range: ${row['MAX_UNIT_PRICE']:.2f} (max)
        
        STATISTICAL CONTEXT:
        - Z-Score: {z_score:.2f} (how many standard deviations from average)
        - Overall Average Invoice: ${row['OVERALL_AVG']:,.2f}
        
        RISK FACTORS TO CONSIDER:
        1. Statistical outliers (Z-score > 2 is suspicious)
        2. Weekend/holiday invoices (unusual timing)
        3. Unusual pricing patterns
        4. Round numbers or suspicious amounts
        
        Provide:
        1. Risk Level: LOW/MEDIUM/HIGH
        2. Risk Score: 0.0-1.0
        3. Key risk factors identified
        4. Recommended actions
        
        Format as: RISK_LEVEL|RISK_SCORE|ANALYSIS
        """
        
        # Use Cortex Complete for analysis
        ai_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as analysis"
        ai_result = conn.query(ai_query)
        ai_response = ai_result.iloc[0]['ANALYSIS'] if not ai_result.empty else "AI analysis unavailable"
        
        # Parse response
        try:
            parts = ai_response.split('|', 2)
            if len(parts) >= 3:
                return {
                    "risk_level": parts[0].strip(),
                    "risk_score": float(parts[1].strip()),
                    "analysis": parts[2].strip(),
                    "z_score": z_score,
                    "day_type": row['DAY_TYPE']
                }
        except:
            pass
        
        # Fallback statistical assessment
        risk_level = "HIGH" if z_score > 3 else "MEDIUM" if z_score > 2 else "LOW"
        risk_score = min(1.0, z_score / 3.0)
        
        return {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "analysis": ai_response,
            "z_score": z_score,
            "day_type": row['DAY_TYPE']
        }
        
    except Exception as e:
        return {"risk_level": "Error", "analysis": f"Analysis failed: {e}", "risk_score": 0}

def ai_invoice_categorization(invoice_id):
    """AI-powered invoice categorization with confidence scoring"""
    try:
        # Get detailed product information
        query = f"""
        SELECT 
            i.invoice_id,
            LISTAGG(DISTINCT i.product_name, ', ') as products,
            COUNT(DISTINCT i.product_name) as unique_products,
            SUM(i.total_price) as total_amount,
            AVG(i.unit_price) as avg_price,
            t.invoice_date
        FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_ITEMS i
        JOIN {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS t ON i.invoice_id = t.invoice_id
        WHERE i.invoice_id = '{invoice_id}'
        GROUP BY i.invoice_id, t.invoice_date
        """
        
        data = conn.query(query)
        
        if data.empty:
            return {"category": "Unknown", "confidence": 0, "reasoning": "No product data found"}
        
        row = data.iloc[0]
        
        prompt = f"""
        Categorize this business invoice based on detailed analysis:
        
        INVOICE DATA:
        - Products: {row['PRODUCTS']}
        - Number of unique products: {row['UNIQUE_PRODUCTS']}
        - Total amount: ${row['TOTAL_AMOUNT']:,.2f}
        - Average item price: ${row['AVG_PRICE']:,.2f}
        - Date: {row['INVOICE_DATE']}
        
        CATEGORIES TO CHOOSE FROM:
        1. Food & Beverages - groceries, restaurant supplies, catering
        2. Office Supplies - stationery, equipment, furniture  
        3. Technology - computers, software, electronics
        4. Manufacturing - raw materials, components, tools
        5. Services - consulting, maintenance, professional services
        6. Travel & Entertainment - hotels, meals, events
        7. Utilities - electricity, water, telecommunications
        8. Other - miscellaneous or mixed categories
        
        Consider:
        - Product types and their business use
        - Spending patterns and amounts
        - Industry context
        
        Respond in JSON format:
        {{"category": "Category Name", "confidence": 0.95, "reasoning": "Detailed explanation"}}
        """
        
        ai_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as response"
        ai_result = conn.query(ai_query)
        ai_response = ai_result.iloc[0]['RESPONSE'] if not ai_result.empty else "{}"
        
        try:
            # Try to parse JSON response
            result = json.loads(ai_response)
            if all(key in result for key in ['category', 'confidence', 'reasoning']):
                return result
        except:
            pass
        
        # Fallback: Simple rule-based categorization
        products_lower = row['PRODUCTS'].lower()
        if any(food in products_lower for food in ['bread', 'milk', 'eggs', 'chicken', 'rice', 'tomatoes']):
            return {"category": "Food & Beverages", "confidence": 0.8, "reasoning": "Contains food items"}
        else:
            return {"category": "Other", "confidence": 0.5, "reasoning": "Could not determine category from AI"}
            
    except Exception as e:
        return {"category": "Error", "confidence": 0, "reasoning": f"Categorization failed: {e}"}

def ai_chatbot_query(user_question):
    """Enhanced AI chatbot with invoice data context"""
    try:
        # Get system context
        context_query = f"""
        SELECT 
            COUNT(DISTINCT t.invoice_id) as total_invoices,
            SUM(t.total) as total_amount,
            AVG(t.total) as avg_amount,
            MIN(t.invoice_date) as earliest_date,
            MAX(t.invoice_date) as latest_date,
            COUNT(DISTINCT CASE WHEN r.review_status = 'Auto-reconciled' THEN t.invoice_id END) as auto_reconciled,
            COUNT(DISTINCT CASE WHEN r.review_status = 'Pending Review' THEN t.invoice_id END) as pending_review
        FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS t
        LEFT JOIN {DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_TOTALS r ON t.invoice_id = r.invoice_id
        """
        
        context = conn.query(context_query).iloc[0]
        
        # Get recent trends
        trend_query = f"""
        SELECT 
            DATE_TRUNC('month', invoice_date) as month,
            COUNT(*) as monthly_invoices,
            SUM(total) as monthly_spend
        FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
        WHERE invoice_date >= DATEADD(month, -6, CURRENT_DATE())
        GROUP BY month
        ORDER BY month DESC
        LIMIT 3
        """
        
        trends = conn.query(trend_query)
        trend_summary = []
        for _, row in trends.iterrows():
            trend_summary.append(f"{row['MONTH'].strftime('%Y-%m')}: {row['MONTHLY_INVOICES']} invoices, ${row['MONTHLY_SPEND']:,.2f}")
        
        prompt = f"""
        You are an AI assistant for an advanced invoice management system with Document AI and Cortex capabilities.
        
        SYSTEM CONTEXT:
        - Total Invoices: {context['TOTAL_INVOICES']:,}
        - Total Value: ${context['TOTAL_AMOUNT']:,.2f}
        - Average Invoice: ${context['AVG_AMOUNT']:,.2f}
        - Date Range: {context['EARLIEST_DATE']} to {context['LATEST_DATE']}
        - Auto-reconciled: {context['AUTO_RECONCILED']:,} invoices
        - Pending Review: {context['PENDING_REVIEW']:,} invoices
        
        RECENT TRENDS:
        {'; '.join(trend_summary)}
        
        CAPABILITIES:
        - Document AI extraction from PDFs
        - Automated reconciliation with discrepancy detection
        - Fraud risk analysis using statistical modeling
        - AI-powered invoice categorization
        - Anomaly detection and pattern analysis
        - Real-time Cortex AI insights
        
        USER QUESTION: {user_question}
        
        Provide a helpful, accurate response. If the question requires specific data analysis, suggest concrete actions or queries that could be performed with the system.
        """
        
        ai_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as response"
        ai_result = conn.query(ai_query)
        return ai_result.iloc[0]['RESPONSE'] if not ai_result.empty else "I apologize, but I'm unable to process your question at the moment."
        
    except Exception as e:
        return f"I encountered an error while processing your question: {e}"

def generate_spend_insights():
    """Generate comprehensive spend analytics using AI"""
    try:
        # Get comprehensive spend data
        analysis_query = f"""
        WITH monthly_analysis AS (
            SELECT 
                DATE_TRUNC('month', invoice_date) as month,
                COUNT(*) as invoice_count,
                SUM(total) as monthly_spend,
                AVG(total) as avg_invoice
            FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
            WHERE invoice_date >= DATEADD(month, -12, CURRENT_DATE())
            GROUP BY month
            ORDER BY month
        ),
        category_analysis AS (
            SELECT 
                CASE 
                    WHEN LOWER(product_name) LIKE '%bread%' OR LOWER(product_name) LIKE '%milk%' 
                         OR LOWER(product_name) LIKE '%eggs%' THEN 'Food & Beverages'
                    WHEN LOWER(product_name) LIKE '%chicken%' OR LOWER(product_name) LIKE '%cheese%' THEN 'Protein Products'
                    WHEN LOWER(product_name) LIKE '%rice%' OR LOWER(product_name) LIKE '%onions%' THEN 'Staple Foods'
                    ELSE 'Other Products'
                END as category,
                COUNT(*) as item_count,
                SUM(total_price) as category_spend,
                AVG(unit_price) as avg_unit_price
            FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_ITEMS
            GROUP BY category
        ),
        risk_analysis AS (
            SELECT 
                COUNT(*) as total_invoices,
                AVG(total) as overall_avg,
                STDDEV(total) as overall_stddev,
                COUNT(CASE WHEN ABS(total - (SELECT AVG(total) FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS)) > 
                              2 * (SELECT STDDEV(total) FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS) THEN 1 END) as outliers
            FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
        )
        SELECT 
            (SELECT LISTAGG(month || ':$' || monthly_spend, ', ') FROM monthly_analysis) as monthly_trends,
            (SELECT LISTAGG(category || ':$' || category_spend, ', ') FROM category_analysis) as category_breakdown,
            (SELECT 'Outliers:' || outliers || '/' || total_invoices FROM risk_analysis) as risk_summary
        """
        
        data = conn.query(analysis_query).iloc[0]
        
        prompt = f"""
        Analyze this comprehensive spend data and provide strategic business insights:
        
        MONTHLY TRENDS (Last 12 months):
        {data['MONTHLY_TRENDS']}
        
        CATEGORY BREAKDOWN:
        {data['CATEGORY_BREAKDOWN']}
        
        RISK SUMMARY:
        {data['RISK_SUMMARY']}
        
        Provide 5-7 key insights covering:
        1. Spending trend analysis (growth, seasonality, patterns)
        2. Category performance and optimization opportunities
        3. Risk and anomaly assessment  
        4. Cost control recommendations
        5. Process improvement suggestions
        6. Strategic recommendations
        
        Format as numbered insights with specific, actionable recommendations.
        """
        
        ai_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as insights"
        ai_result = conn.query(ai_query)
        return ai_result.iloc[0]['INSIGHTS'] if not ai_result.empty else "Unable to generate insights at this time."
        
    except Exception as e:
        return f"Error generating insights: {e}"

# --- Enhanced UI Components ---

def render_ai_dashboard():
    """Render the main AI-powered dashboard"""
    st.header("🤖 AI-Powered Invoice Intelligence Dashboard")
    
    # Key metrics with AI insights
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        # Get enhanced metrics
        metrics_query = f"""
        SELECT 
            COUNT(DISTINCT t.invoice_id) as total_invoices,
            SUM(t.total) as total_amount,
            COUNT(DISTINCT CASE WHEN r.review_status = 'Auto-reconciled' THEN t.invoice_id END) as auto_reconciled,
            COUNT(DISTINCT CASE WHEN ABS(t.total - (SELECT AVG(total) FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS)) > 
                                 2 * (SELECT STDDEV(total) FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS) THEN t.invoice_id END) as anomalies
        FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS t
        LEFT JOIN {DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_TOTALS r ON t.invoice_id = r.invoice_id
        """
        
        metrics = conn.query(metrics_query).iloc[0]
        
        col1.metric(
            "📋 Total Invoices",
            f"{metrics['TOTAL_INVOICES']:,}",
            help="Total invoices in the system"
        )
        
        col2.metric(
            "💰 Total Value", 
            f"${metrics['TOTAL_AMOUNT']:,.2f}",
            help="Combined value of all invoices"
        )
        
        auto_rate = (metrics['AUTO_RECONCILED'] / metrics['TOTAL_INVOICES'] * 100) if metrics['TOTAL_INVOICES'] > 0 else 0
        col3.metric(
            "🤖 Auto-Reconciled",
            f"{metrics['AUTO_RECONCILED']:,}",
            delta=f"{auto_rate:.1f}% of total",
            help="Invoices automatically reconciled by AI"
        )
        
        col4.metric(
            "⚠️ Anomalies Detected",
            f"{metrics['ANOMALIES']:,}",
            delta="Statistical outliers",
            help="Invoices with unusual patterns"
        )
        
    except Exception as e:
        st.error(f"Error loading metrics: {e}")

def render_ai_chatbot():
    """Enhanced AI chatbot interface"""
    st.subheader("💬 AI Invoice Assistant")
    st.caption("Ask me anything about your invoices, patterns, or system capabilities!")
    
    # Display recent chat history
    if st.session_state.chat_history:
        with st.expander("💭 Recent Conversation", expanded=False):
            for i, (role, message) in enumerate(st.session_state.chat_history[-6:]):
                if role == "user":
                    st.markdown(f"**👤 You:** {message}")
                else:
                    st.markdown(f"**🤖 AI:** {message}")
    
    # Chat input with suggestions
    st.markdown("**💡 Try asking:**")
    suggestion_cols = st.columns(3)
    
    suggestions = [
        "What are my spending trends?",
        "Show me any suspicious invoices",
        "How many invoices were auto-reconciled?"
    ]
    
    for i, suggestion in enumerate(suggestions):
        if suggestion_cols[i].button(f"💭 {suggestion}", key=f"suggest_{i}"):
            st.session_state.chat_input = suggestion
    
    # Main chat input
    user_input = st.text_input(
        "Ask your question:",
        placeholder="e.g., 'What categories am I spending the most on?' or 'Any unusual patterns this month?'",
        key="chat_input"
    )
    
    if st.button("🚀 Ask AI", type="primary") and user_input:
        # Add user message
        st.session_state.chat_history.append(("user", user_input))
        
        # Get AI response
        with st.spinner("🧠 AI is analyzing your invoice data..."):
            ai_response = ai_chatbot_query(user_input)
        
        # Add AI response
        st.session_state.chat_history.append(("assistant", ai_response))
        
        # Show latest response immediately
        st.markdown("**🤖 AI Assistant:**")
        st.markdown(ai_response)
        
        # Keep only last 20 messages
        if len(st.session_state.chat_history) > 20:
            st.session_state.chat_history = st.session_state.chat_history[-20:]

def render_enhanced_invoice_analysis():
    """Enhanced invoice analysis with AI features"""
    st.subheader("🔍 Enhanced Invoice Analysis")
    
    # Get list of invoices
    invoice_query = f"""
    SELECT DISTINCT invoice_id 
    FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS 
    ORDER BY invoice_id
    """
    
    try:
        invoices_df = conn.query(invoice_query)
        invoice_list = [""] + invoices_df['INVOICE_ID'].tolist()
        
        selected_invoice_id = st.selectbox("Select Invoice for AI Analysis:", invoice_list, index=0)
        
        if selected_invoice_id:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**🚨 Fraud Risk Analysis**")
                if st.button("🔍 Analyze Fraud Risk", key="fraud_btn"):
                    with st.spinner("AI is analyzing fraud patterns..."):
                        fraud_result = ai_fraud_risk_analysis(selected_invoice_id)
                    
                    # Display results with color coding
                    risk_color = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(fraud_result["risk_level"], "⚪")
                    st.markdown(f"**Risk Level:** {risk_color} {fraud_result['risk_level']}")
                    st.markdown(f"**Risk Score:** {fraud_result.get('risk_score', 0):.2f}/1.0")
                    
                    if 'z_score' in fraud_result:
                        st.markdown(f"**Statistical Z-Score:** {fraud_result['z_score']:.2f}")
                    
                    st.markdown("**AI Analysis:**")
                    st.write(fraud_result["analysis"])
            
            with col2:
                st.markdown("**🏷️ AI Categorization**")
                if st.button("🎯 Categorize Invoice", key="category_btn"):
                    with st.spinner("AI is categorizing invoice..."):
                        category_result = ai_invoice_categorization(selected_invoice_id)
                    
                    st.markdown(f"**Category:** {category_result['category']}")
                    st.markdown(f"**Confidence:** {category_result['confidence']:.1%}")
                    st.markdown("**Reasoning:**")
                    st.write(category_result["reasoning"])
                    
    except Exception as e:
        st.error(f"Error loading invoices: {e}")

def render_spend_analytics():
    """Enhanced spend analytics with AI insights"""
    st.header("📊 AI-Powered Spend Analytics")
    
    tab1, tab2, tab3 = st.tabs(["📈 Trends & Insights", "🔍 Anomaly Detection", "💡 AI Recommendations"])
    
    with tab1:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Create spend trend visualization
            try:
                trend_query = f"""
                SELECT 
                    DATE_TRUNC('month', invoice_date) as month,
                    COUNT(*) as invoice_count,
                    SUM(total) as total_spend,
                    AVG(total) as avg_invoice
                FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
                WHERE invoice_date >= DATEADD(month, -12, CURRENT_DATE())
                GROUP BY month
                ORDER BY month
                """
                
                trend_data = conn.query(trend_query)
                
                if not trend_data.empty:
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    
                    fig.add_trace(
                        go.Scatter(x=trend_data['MONTH'], y=trend_data['TOTAL_SPEND'],
                                 mode='lines+markers', name='Total Spend ($)',
                                 line=dict(color='#1f77b4', width=3)),
                    )
                    
                    fig.add_trace(
                        go.Bar(x=trend_data['MONTH'], y=trend_data['INVOICE_COUNT'],
                               name='Invoice Count', opacity=0.7,
                               marker_color='#ff7f0e'),
                        secondary_y=True
                    )
                    
                    fig.update_layout(
                        title="📈 Monthly Spend Trends",
                        height=400,
                        hovermode='x'
                    )
                    
                    fig.update_yaxes(title_text="Total Spend ($)", secondary_y=False)
                    fig.update_yaxes(title_text="Invoice Count", secondary_y=True)
                    
                    st.plotly_chart(fig, use_container_width=True)
                
            except Exception as e:
                st.error(f"Error creating trend visualization: {e}")
        
        with col2:
            st.markdown("**📊 Quick Stats**")
            
            if st.button("🧠 Generate AI Insights"):
                with st.spinner("AI is analyzing spend patterns..."):
                    insights = generate_spend_insights()
                
                st.markdown("**💡 AI Insights:**")
                st.write(insights)
    
    with tab2:
        st.subheader("🔍 Statistical Anomaly Detection")
        
        if st.button("🚨 Detect Anomalies"):
            try:
                anomaly_query = f"""
                WITH stats AS (
                    SELECT AVG(total) as avg_total, STDDEV(total) as stddev_total
                    FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS
                ),
                outliers AS (
                    SELECT 
                        t.invoice_id,
                        t.total,
                        t.invoice_date,
                        s.avg_total,
                        (t.total - s.avg_total) / s.stddev_total as z_score
                    FROM {DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS t
                    CROSS JOIN stats s
                    WHERE ABS((t.total - s.avg_total) / s.stddev_total) > 2
                    ORDER BY ABS(z_score) DESC
                )
                SELECT * FROM outliers LIMIT 10
                """
                
                anomalies = conn.query(anomaly_query)
                
                if not anomalies.empty:
                    st.markdown("**⚠️ Statistical Outliers Found:**")
                    
                    for _, row in anomalies.iterrows():
                        severity = "🔴 Critical" if abs(row['Z_SCORE']) > 3 else "🟡 Medium"
                        
                        with st.expander(f"{severity} - Invoice {row['INVOICE_ID']} (Z-score: {row['Z_SCORE']:.2f})"):
                            col1, col2, col3 = st.columns(3)
                            col1.metric("Amount", f"${row['TOTAL']:,.2f}")
                            col2.metric("Expected Avg", f"${row['AVG_TOTAL']:,.2f}")
                            col3.metric("Deviation", f"{abs(row['Z_SCORE']):.1f}σ")
                            
                            st.write(f"**Date:** {row['INVOICE_DATE']}")
                else:
                    st.success("✅ No statistical anomalies detected!")
                    
            except Exception as e:
                st.error(f"Error detecting anomalies: {e}")
    
    with tab3:
        st.subheader("💡 AI-Powered Recommendations")
        st.info("This section provides AI-generated recommendations for process improvements and cost optimization.")
        
        if st.button("🎯 Get AI Recommendations"):
            with st.spinner("AI is generating recommendations..."):
                recommendations = generate_spend_insights()
            
            st.markdown("**🚀 Strategic Recommendations:**")
            st.write(recommendations)

# --- Main Application ---

def main():
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
            "🏠 AI Dashboard",
            "🔍 Enhanced Invoice Analysis", 
            "💬 AI Assistant",
            "📊 Advanced Analytics"
        ])
        
        st.divider()
        
        # System info
        st.markdown("### ℹ️ System Info")
        st.info("🚀 Running on Streamlit in Snowflake")
        st.success("✅ Cortex AI Enabled")
        st.success("✅ Document AI Ready")
    
    # Route to different pages
    if page == "🏠 AI Dashboard":
        render_ai_dashboard()
        
        st.divider()
        
        # Quick action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**🔍 Invoice Analysis**")
            st.caption("AI-powered fraud detection and categorization")
        
        with col2:
            st.markdown("**💬 AI Assistant**")
            st.caption("Natural language invoice queries")
        
        with col3:
            st.markdown("**📊 Analytics**")
            st.caption("Interactive visualizations and insights")
    
    elif page == "💬 AI Assistant":
        render_ai_chatbot()
    
    elif page == "🔍 Enhanced Invoice Analysis":
        render_enhanced_invoice_analysis()
    
    elif page == "📊 Advanced Analytics":
        render_spend_analytics()

if __name__ == "__main__":
    main() 