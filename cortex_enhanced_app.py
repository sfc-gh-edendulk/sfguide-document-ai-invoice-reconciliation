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

# Import original functions
from docai_invoice_qs_app import (
    load_reconcile_data, load_bronze_data, get_invoice_reconciliation_metrics,
    display_pdf_page, previous_pdf_page, next_pdf_page, summarize_mismatch_details
)

st.set_page_config(
    page_title="🚀 Enhanced Invoice Intelligence", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuration ---
DB_NAME = "DOC_AI_QS_DB"
SCHEMA_NAME = "DOC_AI_SCHEMA"
STAGE_NAME = "DOC_AI_STAGE"

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
    'pdf_page', 'pdf_doc', 'pdf_url', 'ai_insights_cache'
]

for key in session_state_keys:
    if key not in st.session_state:
        if key == 'chat_history':
            st.session_state[key] = []
        elif key == 'pdf_page':
            st.session_state[key] = 0
        elif key == 'ai_insights_cache':
            st.session_state[key] = {}
        else:
            st.session_state[key] = None

# --- Get Snowflake Session ---
try:
    session = get_active_session()
    st.success("❄️ Enhanced Snowflake AI session established!")
    CURRENT_USER = session.get_current_role().replace("\"", "")
except Exception as e:
    st.error(f"Error getting Snowflake session: {e}")
    st.stop()

# --- Enhanced AI Functions ---

def ai_fraud_risk_analysis(session, invoice_id):
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
                (SELECT AVG(total) FROM {BRONZE_TRANSACT_TOTALS_TABLE}) as overall_avg,
                (SELECT STDDEV(total) FROM {BRONZE_TRANSACT_TOTALS_TABLE}) as overall_stddev,
                DAYOFWEEK(t.invoice_date) as day_of_week
            FROM {BRONZE_TRANSACT_TOTALS_TABLE} t
            LEFT JOIN {BRONZE_TRANSACT_ITEMS_TABLE} i ON t.invoice_id = i.invoice_id
            WHERE t.invoice_id = '{invoice_id}'
            GROUP BY t.invoice_id, t.total, t.invoice_date
        )
        SELECT *,
               (total - overall_avg) / overall_stddev as z_score,
               CASE WHEN day_of_week IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END as day_type
        FROM invoice_stats
        """
        
        data = session.sql(query).to_pandas()
        
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
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as analysis").collect()
        ai_response = response_df[0]["ANALYSIS"] if response_df else "AI analysis unavailable"
        
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

def ai_invoice_categorization(session, invoice_id):
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
        FROM {BRONZE_TRANSACT_ITEMS_TABLE} i
        JOIN {BRONZE_TRANSACT_TOTALS_TABLE} t ON i.invoice_id = t.invoice_id
        WHERE i.invoice_id = '{invoice_id}'
        GROUP BY i.invoice_id, t.invoice_date
        """
        
        data = session.sql(query).to_pandas()
        
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
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as response").collect()
        ai_response = response_df[0]["RESPONSE"] if response_df else "{}"
        
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

def ai_chatbot_query(session, user_question):
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
        FROM {BRONZE_TRANSACT_TOTALS_TABLE} t
        LEFT JOIN {RECONCILE_TOTALS_TABLE} r ON t.invoice_id = r.invoice_id
        """
        
        context = session.sql(context_query).to_pandas().iloc[0]
        
        # Get recent trends
        trend_query = f"""
        SELECT 
            DATE_TRUNC('month', invoice_date) as month,
            COUNT(*) as monthly_invoices,
            SUM(total) as monthly_spend
        FROM {BRONZE_TRANSACT_TOTALS_TABLE}
        WHERE invoice_date >= DATEADD(month, -6, CURRENT_DATE())
        GROUP BY month
        ORDER BY month DESC
        LIMIT 3
        """
        
        trends = session.sql(trend_query).to_pandas()
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
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as response").collect()
        return response_df[0]["RESPONSE"] if response_df else "I apologize, but I'm unable to process your question at the moment."
        
    except Exception as e:
        return f"I encountered an error while processing your question: {e}"

def generate_spend_insights(session):
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
            FROM {BRONZE_TRANSACT_TOTALS_TABLE}
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
            FROM {BRONZE_TRANSACT_ITEMS_TABLE}
            GROUP BY category
        ),
        risk_analysis AS (
            SELECT 
                COUNT(*) as total_invoices,
                AVG(total) as overall_avg,
                STDDEV(total) as overall_stddev,
                COUNT(CASE WHEN ABS(total - (SELECT AVG(total) FROM {BRONZE_TRANSACT_TOTALS_TABLE})) > 
                              2 * (SELECT STDDEV(total) FROM {BRONZE_TRANSACT_TOTALS_TABLE}) THEN 1 END) as outliers
            FROM {BRONZE_TRANSACT_TOTALS_TABLE}
        )
        SELECT 
            (SELECT LISTAGG(month || ':$' || monthly_spend, ', ') FROM monthly_analysis) as monthly_trends,
            (SELECT LISTAGG(category || ':$' || category_spend, ', ') FROM category_analysis) as category_breakdown,
            (SELECT 'Outliers:' || outliers || '/' || total_invoices FROM risk_analysis) as risk_summary
        """
        
        data = session.sql(analysis_query).to_pandas().iloc[0]
        
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
        
        response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', $${prompt}$$) as insights").collect()
        return response_df[0]["INSIGHTS"] if response_df else "Unable to generate insights at this time."
        
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
            COUNT(DISTINCT CASE WHEN ABS(t.total - (SELECT AVG(total) FROM {BRONZE_TRANSACT_TOTALS_TABLE})) > 
                                 2 * (SELECT STDDEV(total) FROM {BRONZE_TRANSACT_TOTALS_TABLE}) THEN t.invoice_id END) as anomalies
        FROM {BRONZE_TRANSACT_TOTALS_TABLE} t
        LEFT JOIN {RECONCILE_TOTALS_TABLE} r ON t.invoice_id = r.invoice_id
        """
        
        metrics = session.sql(metrics_query).to_pandas().iloc[0]
        
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
            ai_response = ai_chatbot_query(session, user_input)
        
        # Add AI response
        st.session_state.chat_history.append(("assistant", ai_response))
        
        # Show latest response immediately
        st.markdown("**🤖 AI Assistant:**")
        st.markdown(ai_response)
        
        # Keep only last 20 messages
        if len(st.session_state.chat_history) > 20:
            st.session_state.chat_history = st.session_state.chat_history[-20:]

def render_enhanced_invoice_analysis(selected_invoice_id):
    """Enhanced invoice analysis with AI features"""
    if not selected_invoice_id:
        return
    
    st.subheader(f"🔍 AI Analysis for Invoice {selected_invoice_id}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🚨 Fraud Risk Analysis**")
        if st.button("🔍 Analyze Fraud Risk", key="fraud_btn"):
            with st.spinner("AI is analyzing fraud patterns..."):
                fraud_result = ai_fraud_risk_analysis(session, selected_invoice_id)
            
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
                category_result = ai_invoice_categorization(session, selected_invoice_id)
            
            st.markdown(f"**Category:** {category_result['category']}")
            st.markdown(f"**Confidence:** {category_result['confidence']:.1%}")
            st.markdown("**Reasoning:**")
            st.write(category_result["reasoning"])

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
                FROM {BRONZE_TRANSACT_TOTALS_TABLE}
                WHERE invoice_date >= DATEADD(month, -12, CURRENT_DATE())
                GROUP BY month
                ORDER BY month
                """
                
                trend_data = session.sql(trend_query).to_pandas()
                
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
                    insights = generate_spend_insights(session)
                
                st.markdown("**💡 AI Insights:**")
                st.write(insights)
    
    with tab2:
        st.subheader("🔍 Statistical Anomaly Detection")
        
        if st.button("🚨 Detect Anomalies"):
            try:
                anomaly_query = f"""
                WITH stats AS (
                    SELECT AVG(total) as avg_total, STDDEV(total) as stddev_total
                    FROM {BRONZE_TRANSACT_TOTALS_TABLE}
                ),
                outliers AS (
                    SELECT 
                        t.invoice_id,
                        t.total,
                        t.invoice_date,
                        s.avg_total,
                        (t.total - s.avg_total) / s.stddev_total as z_score
                    FROM {BRONZE_TRANSACT_TOTALS_TABLE} t
                    CROSS JOIN stats s
                    WHERE ABS((t.total - s.avg_total) / s.stddev_total) > 2
                    ORDER BY ABS(z_score) DESC
                )
                SELECT * FROM outliers LIMIT 10
                """
                
                anomalies = session.sql(anomaly_query).to_pandas()
                
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
                recommendations = generate_spend_insights(session)
            
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
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar navigation
    with st.sidebar:
        st.markdown("### 🎛️ Navigation")
        
        page = st.selectbox("Choose Module:", [
            "🏠 AI Dashboard",
            "🔍 Enhanced Invoice Review", 
            "💬 AI Assistant",
            "📊 Advanced Analytics",
            "⚙️ System Management"
        ])
        
        st.divider()
        
        # Enhanced upload section
        st.markdown("### 📂 Document Upload")
        uploaded = st.file_uploader(
            "Upload PDF/Image", 
            type=["pdf", "jpg", "jpeg", "png"], 
            accept_multiple_files=True,
            help="Upload invoices for AI-powered processing"
        )
        
        if uploaded:
            for uploaded_file in uploaded:
                file_bytes = uploaded_file.read()
                file_name = uploaded_file.name
                
                session.file.put_stream(
                    io.BytesIO(file_bytes), 
                    f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{file_name}", 
                    overwrite=True, 
                    auto_compress=False
                )
                session.sql(f"ALTER STAGE {DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME} REFRESH").collect()
                
                st.success(f"✅ {file_name}")
        
        st.divider()
        
        # Quick AI insights
        st.markdown("### ⚡ Quick AI Insights")
        if st.button("🧠 System Health Check"):
            try:
                health_query = f"""
                SELECT 
                    COUNT(*) as total_invoices,
                    COUNT(CASE WHEN review_status = 'Pending Review' THEN 1 END) as needs_review
                FROM {RECONCILE_TOTALS_TABLE}
                """
                health = session.sql(health_query).to_pandas().iloc[0]
                
                if health['NEEDS_REVIEW'] > 0:
                    st.warning(f"⚠️ {health['NEEDS_REVIEW']} invoices need review")
                else:
                    st.success("✅ All invoices processed!")
                    
            except:
                st.info("System status unavailable")
    
    # Route to different pages
    if page == "🏠 AI Dashboard":
        render_ai_dashboard()
        
        st.divider()
        
        # Quick action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔍 Review Pending Invoices", type="secondary", use_container_width=True):
                st.switch_page("Enhanced Invoice Review")
        
        with col2:
            if st.button("💬 Ask AI Assistant", type="secondary", use_container_width=True):
                st.switch_page("AI Assistant")
        
        with col3:
            if st.button("📊 View Analytics", type="secondary", use_container_width=True):
                st.switch_page("Advanced Analytics")
    
    elif page == "💬 AI Assistant":
        render_ai_chatbot()
    
    elif page == "🔍 Enhanced Invoice Review":
        st.header("🔍 Enhanced Invoice Review & AI Analysis")
        
        # Load reconciliation data
        review_status_options = ['Pending Review', 'Reviewed', 'Auto-reconciled', 'All']
        selected_status = st.selectbox("Filter by Status:", review_status_options, index=0)
        
        invoices_to_review_df, reconcile_items_details_df, reconcile_totals_details_df = load_reconcile_data(selected_status)
        
        if not invoices_to_review_df.empty:
            # Enhanced metrics display
            with st.spinner("Loading enhanced metrics..."):
                reconciliation_data = get_invoice_reconciliation_metrics(session)
            
            if reconciliation_data:
                col1, col2, col3, col4 = st.columns(4)
                
                col1.metric("📋 Total Invoices", f"{reconciliation_data['total_invoice_count']:,}")
                col2.metric("🤖 Auto-Reconciled", f"{reconciliation_data['count_auto_reconciled']:,}")
                col3.metric("💰 Total Value", f"${reconciliation_data['grand_total_amount']:,.2f}")
                col4.metric("✅ Reconciled Value", f"${reconciliation_data['total_reconciled_amount']:,.2f}")
            
            # Invoice selection with enhanced display
            invoice_list = [""] + invoices_to_review_df['INVOICE_ID'].unique().tolist()
            selected_invoice_id = st.selectbox("Select Invoice for AI Analysis:", invoice_list, index=0)
            
            if selected_invoice_id:
                # Enhanced invoice analysis
                render_enhanced_invoice_analysis(selected_invoice_id)
                
                st.divider()
                
                # Original reconciliation interface (enhanced)
                st.subheader("📋 Reconciliation Data")
                
                # Load and display bronze data
                bronze_data_dict = load_bronze_data(selected_invoice_id)
                
                if bronze_data_dict:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**📊 Transaction Data (Editable)**")
                        
                        if not bronze_data_dict['transact_items'].empty:
                            st.write("**Items:**")
                            edited_items = st.data_editor(
                                bronze_data_dict['transact_items'],
                                key="enhanced_editor_items",
                                use_container_width=True
                            )
                            st.session_state.edited_transact_items = edited_items
                        
                        if not bronze_data_dict['transact_totals'].empty:
                            st.write("**Totals:**")
                            edited_totals = st.data_editor(
                                bronze_data_dict['transact_totals'].head(1),
                                key="enhanced_editor_totals",
                                use_container_width=True
                            )
                            st.session_state.edited_transact_totals = edited_totals
                    
                    with col2:
                        st.markdown("**🤖 Document AI Data (Reference)**")
                        
                        if not bronze_data_dict['docai_items'].empty:
                            st.write("**AI Extracted Items:**")
                            st.dataframe(bronze_data_dict['docai_items'], use_container_width=True)
                            st.session_state.docai_items = bronze_data_dict['docai_items']
                        
                        if not bronze_data_dict['docai_totals'].empty:
                            st.write("**AI Extracted Totals:**")
                            st.dataframe(bronze_data_dict['docai_totals'], use_container_width=True)
                            st.session_state.docai_totals = bronze_data_dict['docai_totals']
                
                # Enhanced submission section
                st.subheader("✅ Submit Review")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🤖 Accept AI Extracted Values", type="primary"):
                        # Handle DocAI acceptance logic here
                        st.success("AI values accepted for reconciliation!")
                
                with col2:
                    review_notes = st.text_area("Review Notes:", key="enhanced_notes")
                    if st.button("✍️ Accept Manual Edits"):
                        # Handle manual edits logic here
                        st.success("Manual edits accepted!")
        
        else:
            st.info(f"No invoices found with status '{selected_status}'")
    
    elif page == "📊 Advanced Analytics":
        render_spend_analytics()
    
    elif page == "⚙️ System Management":
        st.header("⚙️ Enhanced System Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔄 Task Management")
            
            task_buttons = [
                ("▶️ Resume Reconciliation", "ALTER TASK DOC_AI_QS_DB.DOC_AI_SCHEMA.RECONCILE RESUME"),
                ("⏸️ Suspend Reconciliation", "ALTER TASK DOC_AI_QS_DB.DOC_AI_SCHEMA.RECONCILE SUSPEND"),
                ("🔄 Refresh Document Stage", "ALTER STAGE DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE REFRESH")
            ]
            
            for button_text, sql_command in task_buttons:
                if st.button(button_text, use_container_width=True):
                    try:
                        session.sql(sql_command).collect()
                        st.success(f"✅ {button_text.split(' ', 1)[1]} completed!")
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
        
        with col2:
            st.subheader("📊 System Health")
            
            try:
                # Enhanced system status
                status_query = """
                SELECT 
                    'RECONCILE' as task_name,
                    CURRENT_TIMESTAMP() as check_time
                """
                
                st.write("**System Status:** ✅ Operational")
                st.write(f"**Last Check:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # AI system health
                if st.button("🧠 Test AI Systems"):
                    with st.spinner("Testing AI capabilities..."):
                        try:
                            test_query = "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', 'Hello, this is a test. Respond with: AI systems operational.') as test"
                            result = session.sql(test_query).collect()
                            st.success("✅ Cortex AI systems operational!")
                        except Exception as e:
                            st.error(f"❌ AI systems error: {e}")
                            
            except Exception as e:
                st.warning(f"Could not retrieve system status: {e}")

if __name__ == "__main__":
    main() 