-- ===============================================
-- ENHANCED INVOICE INTELLIGENCE DEMO SCRIPT
-- Quick tests to showcase AI capabilities
-- ===============================================

USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

-- ===============================================
-- 1. BASIC SYSTEM CHECK
-- ===============================================

-- Check if Cortex AI is available
SELECT 'Cortex AI Status' as test_name, 
       SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', 'Respond with: AI systems operational') as result;

-- Check existing invoice data
SELECT 'Invoice Data Status' as test_name,
       COUNT(*) as total_invoices,
       SUM(total) as total_amount,
       AVG(total) as avg_amount
FROM TRANSACT_TOTALS;

-- ===============================================
-- 2. AI FRAUD RISK DEMO
-- ===============================================

-- Test AI fraud analysis on a high-value invoice
WITH fraud_test AS (
    SELECT 
        invoice_id,
        total,
        (SELECT AVG(total) FROM TRANSACT_TOTALS) as avg_total,
        (SELECT STDDEV(total) FROM TRANSACT_TOTALS) as stddev_total
    FROM TRANSACT_TOTALS
    WHERE invoice_id = '2004' -- Known outlier with tax modification
)
SELECT 
    'Fraud Risk Demo' as test_name,
    invoice_id,
    total as invoice_amount,
    avg_total,
    ((total - avg_total) / stddev_total) as z_score,
    CASE 
        WHEN ABS((total - avg_total) / stddev_total) > 3 THEN '🔴 HIGH RISK'
        WHEN ABS((total - avg_total) / stddev_total) > 2 THEN '🟡 MEDIUM RISK'
        ELSE '🟢 LOW RISK'
    END as risk_assessment
FROM fraud_test;

-- ===============================================
-- 3. AI CATEGORIZATION DEMO
-- ===============================================

-- Test AI categorization
WITH category_test AS (
    SELECT 
        invoice_id,
        LISTAGG(product_name, ', ') as products,
        SUM(total_price) as total_amount
    FROM TRANSACT_ITEMS
    WHERE invoice_id = '2001'
    GROUP BY invoice_id
)
SELECT 
    'AI Categorization Demo' as test_name,
    invoice_id,
    products,
    total_amount,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-8b',
        'Categorize this invoice: Products=' || products || ', Amount=$' || total_amount || 
        '. Choose: Food & Beverages, Office Supplies, Technology, Services, Other. Respond with just the category name.'
    ) as ai_category
FROM category_test;

-- ===============================================
-- 4. ANOMALY DETECTION DEMO
-- ===============================================

-- Find statistical anomalies in invoice amounts
WITH anomaly_detection AS (
    SELECT 
        invoice_id,
        total,
        invoice_date,
        AVG(total) OVER() as overall_avg,
        STDDEV(total) OVER() as overall_stddev,
        (total - AVG(total) OVER()) / STDDEV(total) OVER() as z_score
    FROM TRANSACT_TOTALS
)
SELECT 
    'Anomaly Detection Demo' as test_name,
    invoice_id,
    total as amount,
    ROUND(z_score, 2) as z_score,
    CASE 
        WHEN ABS(z_score) > 3 THEN '🚨 CRITICAL ANOMALY'
        WHEN ABS(z_score) > 2 THEN '⚠️ MODERATE ANOMALY'
        ELSE '✅ NORMAL'
    END as anomaly_status
FROM anomaly_detection
WHERE ABS(z_score) > 2
ORDER BY ABS(z_score) DESC
LIMIT 5;

-- ===============================================
-- 5. SPEND ANALYTICS DEMO
-- ===============================================

-- Monthly spend trends analysis
SELECT 
    'Monthly Trends Demo' as test_name,
    DATE_TRUNC('month', invoice_date) as month,
    COUNT(*) as invoice_count,
    SUM(total) as monthly_spend,
    ROUND(AVG(total), 2) as avg_invoice_amount
FROM TRANSACT_TOTALS
GROUP BY DATE_TRUNC('month', invoice_date)
ORDER BY month;

-- Category breakdown
WITH category_analysis AS (
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
        ROUND(AVG(unit_price), 2) as avg_unit_price
    FROM TRANSACT_ITEMS
    GROUP BY category
)
SELECT 
    'Category Analysis Demo' as test_name,
    category,
    item_count,
    category_spend,
    avg_unit_price,
    ROUND(category_spend / SUM(category_spend) OVER() * 100, 1) as percent_of_total
FROM category_analysis
ORDER BY category_spend DESC;

-- ===============================================
-- 6. AI INSIGHTS DEMO
-- ===============================================

-- Generate AI-powered business insights
WITH spend_summary AS (
    SELECT 
        COUNT(DISTINCT invoice_id) as total_invoices,
        SUM(total) as total_spend,
        ROUND(AVG(total), 2) as avg_invoice,
        MIN(invoice_date) as earliest_date,
        MAX(invoice_date) as latest_date
    FROM TRANSACT_TOTALS
)
SELECT 
    'AI Business Insights Demo' as test_name,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-8b',
        'Analyze this business spend data and provide 3 key insights: ' ||
        'Total Invoices: ' || total_invoices ||
        ', Total Spend: $' || total_spend ||
        ', Average Invoice: $' || avg_invoice ||
        ', Date Range: ' || earliest_date || ' to ' || latest_date ||
        '. Focus on business patterns and optimization opportunities.'
    ) as ai_insights
FROM spend_summary;

-- ===============================================
-- 7. RECONCILIATION STATUS DEMO
-- ===============================================

-- Enhanced reconciliation metrics
SELECT 
    'Reconciliation Status Demo' as test_name,
    COUNT(DISTINCT t.invoice_id) as total_invoices,
    COUNT(DISTINCT CASE WHEN ri.review_status = 'Auto-reconciled' THEN t.invoice_id END) as auto_reconciled_items,
    COUNT(DISTINCT CASE WHEN rt.review_status = 'Auto-reconciled' THEN t.invoice_id END) as auto_reconciled_totals,
    COUNT(DISTINCT CASE WHEN ri.review_status = 'Pending Review' THEN t.invoice_id END) as pending_items,
    COUNT(DISTINCT CASE WHEN rt.review_status = 'Pending Review' THEN t.invoice_id END) as pending_totals,
    ROUND(
        COUNT(DISTINCT CASE WHEN ri.review_status = 'Auto-reconciled' AND rt.review_status = 'Auto-reconciled' THEN t.invoice_id END) 
        / COUNT(DISTINCT t.invoice_id) * 100, 1
    ) as full_auto_reconciliation_rate
FROM TRANSACT_TOTALS t
LEFT JOIN RECONCILE_RESULTS_ITEMS ri ON t.invoice_id = ri.invoice_id
LEFT JOIN RECONCILE_RESULTS_TOTALS rt ON t.invoice_id = rt.invoice_id;

-- ===============================================
-- 8. SYSTEM HEALTH CHECK
-- ===============================================

-- Check task status
SHOW TASKS LIKE '%RECONCILE%';

-- Check recent task runs
SELECT 
    'Task Health Check' as test_name,
    name as task_name,
    state as task_state,
    next_scheduled_time,
    last_run_time
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name LIKE '%RECONCILE%'
ORDER BY last_run_time DESC
LIMIT 5;

-- ===============================================
-- DEMO SUMMARY
-- ===============================================

SELECT 
    '🎉 DEMO COMPLETE' as status,
    'Enhanced Invoice Intelligence Platform is ready!' as message,
    'Key Features: AI Fraud Detection, Smart Categorization, Anomaly Detection, Conversational Analytics' as capabilities;

-- Next steps message
SELECT 
    '🚀 NEXT STEPS' as action,
    'Run: streamlit run cortex_enhanced_app.py' as command,
    'Then explore: AI Dashboard, AI Assistant, Enhanced Analytics' as features; 