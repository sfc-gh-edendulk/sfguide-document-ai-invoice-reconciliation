-- Enhanced Invoice Intelligence with Cortex AI Features
-- Run this script after the basic setup to add advanced AI capabilities

USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

-- ===============================================
-- ENHANCED TABLES FOR AI FEATURES
-- ===============================================

-- Invoice Risk Assessment Table
CREATE OR REPLACE TABLE INVOICE_RISK_ASSESSMENT (
    invoice_id VARCHAR(255),
    risk_score NUMBER(3,2), -- 0.00 to 1.00
    risk_level VARCHAR(10), -- Low, Medium, High
    risk_factors VARIANT, -- JSON of detected risk factors
    ai_analysis TEXT,
    assessed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    assessed_by VARCHAR(255)
);

-- Invoice Categories Table (AI-powered categorization)
CREATE OR REPLACE TABLE INVOICE_CATEGORIES (
    invoice_id VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    confidence_score NUMBER(3,2),
    ai_reasoning TEXT,
    categorized_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    reviewed_by VARCHAR(255)
);

-- Vendor Analysis Table
CREATE OR REPLACE TABLE VENDOR_ANALYSIS (
    vendor_name VARCHAR(255),
    total_invoices NUMBER,
    total_spend NUMBER(12,2),
    avg_invoice_amount NUMBER(12,2),
    risk_score NUMBER(3,2),
    payment_reliability VARCHAR(20),
    last_analysis_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- AI Chat History Table
CREATE OR REPLACE TABLE AI_CHAT_HISTORY (
    chat_id VARCHAR(255),
    user_id VARCHAR(255),
    user_message TEXT,
    ai_response TEXT,
    context_data VARIANT,
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Anomaly Detection Results
CREATE OR REPLACE TABLE ANOMALY_DETECTION_RESULTS (
    detection_id VARCHAR(255),
    invoice_id VARCHAR(255),
    anomaly_type VARCHAR(100), -- amount, timing, pattern, etc.
    severity VARCHAR(20), -- Low, Medium, High, Critical
    description TEXT,
    statistical_metrics VARIANT,
    detection_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved BOOLEAN DEFAULT FALSE
);

-- ===============================================
-- AI-POWERED STORED PROCEDURES
-- ===============================================

-- Procedure: Batch Fraud Risk Assessment
CREATE OR REPLACE PROCEDURE SP_BATCH_FRAUD_ASSESSMENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    processed_count NUMBER := 0;
    current_timestamp TIMESTAMP_NTZ;
BEGIN
    current_timestamp := CURRENT_TIMESTAMP();
    
    -- Process invoices that haven't been assessed yet
    FOR invoice_record IN (
        SELECT DISTINCT t.invoice_id, t.total, t.invoice_date
        FROM TRANSACT_TOTALS t
        LEFT JOIN INVOICE_RISK_ASSESSMENT r ON t.invoice_id = r.invoice_id
        WHERE r.invoice_id IS NULL
        LIMIT 100 -- Process in batches
    ) DO
        
        -- Calculate basic risk metrics
        LET avg_amount NUMBER := (SELECT AVG(total) FROM TRANSACT_TOTALS);
        LET stddev_amount NUMBER := (SELECT STDDEV(total) FROM TRANSACT_TOTALS);
        LET z_score NUMBER := (invoice_record.total - avg_amount) / stddev_amount;
        
        -- Determine risk level based on statistical analysis
        LET risk_level VARCHAR := CASE 
            WHEN ABS(z_score) > 3 THEN 'High'
            WHEN ABS(z_score) > 2 THEN 'Medium'
            ELSE 'Low'
        END;
        
        LET risk_score NUMBER := LEAST(1.0, ABS(z_score) / 3.0);
        
        -- Use Cortex for detailed analysis
        LET ai_prompt VARCHAR := 'Analyze this invoice for fraud risk: ID=' || invoice_record.invoice_id || 
                                ', Amount=$' || invoice_record.total || 
                                ', Date=' || invoice_record.invoice_date ||
                                ', Z-score=' || z_score || 
                                '. Provide brief risk assessment.';
        
        LET ai_analysis VARCHAR;
        BEGIN
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', ai_prompt) INTO ai_analysis;
        EXCEPTION
            WHEN OTHER THEN
                ai_analysis := 'AI analysis unavailable';
        END;
        
        -- Insert risk assessment
        INSERT INTO INVOICE_RISK_ASSESSMENT (
            invoice_id, risk_score, risk_level, ai_analysis, assessed_by
        ) VALUES (
            invoice_record.invoice_id, risk_score, risk_level, ai_analysis, 'AUTO_BATCH'
        );
        
        processed_count := processed_count + 1;
    END FOR;
    
    RETURN 'Processed ' || processed_count || ' invoices for fraud risk assessment.';
END;
$$;

-- Procedure: AI-Powered Invoice Categorization
CREATE OR REPLACE PROCEDURE SP_AI_CATEGORIZE_INVOICES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    processed_count NUMBER := 0;
BEGIN
    -- Process uncategorized invoices
    FOR invoice_record IN (
        SELECT 
            i.invoice_id,
            LISTAGG(i.product_name, ', ') as product_list,
            SUM(i.total_price) as total_amount
        FROM TRANSACT_ITEMS i
        LEFT JOIN INVOICE_CATEGORIES c ON i.invoice_id = c.invoice_id
        WHERE c.invoice_id IS NULL
        GROUP BY i.invoice_id
        LIMIT 50
    ) DO
        
        -- Create categorization prompt
        LET ai_prompt VARCHAR := 'Categorize this invoice: Products=' || invoice_record.product_list || 
                                ', Total=$' || invoice_record.total_amount || 
                                '. Choose from: Office Supplies, Food & Beverages, Technology, Services, Travel, Manufacturing, Other. ' ||
                                'Respond with: Category|Confidence(0-1)|Reason';
        
        LET ai_response VARCHAR;
        BEGIN
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', ai_prompt) INTO ai_response;
            
            -- Parse AI response (simplified - in production would use more robust parsing)
            LET category VARCHAR := SPLIT_PART(ai_response, '|', 1);
            LET confidence VARCHAR := SPLIT_PART(ai_response, '|', 2);
            LET reasoning VARCHAR := SPLIT_PART(ai_response, '|', 3);
            
            INSERT INTO INVOICE_CATEGORIES (
                invoice_id, category, confidence_score, ai_reasoning, reviewed_by
            ) VALUES (
                invoice_record.invoice_id, category, TRY_CAST(confidence AS NUMBER(3,2)), reasoning, 'AUTO_AI'
            );
            
            processed_count := processed_count + 1;
            
        EXCEPTION
            WHEN OTHER THEN
                -- Insert default category if AI fails
                INSERT INTO INVOICE_CATEGORIES (
                    invoice_id, category, confidence_score, ai_reasoning, reviewed_by
                ) VALUES (
                    invoice_record.invoice_id, 'Other', 0.5, 'AI categorization failed', 'AUTO_FALLBACK'
                );
        END;
    END FOR;
    
    RETURN 'Categorized ' || processed_count || ' invoices using AI.';
END;
$$;

-- Procedure: Anomaly Detection with Statistical Analysis
CREATE OR REPLACE PROCEDURE SP_DETECT_ANOMALIES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    anomaly_count NUMBER := 0;
    detection_id VARCHAR;
BEGIN
    detection_id := 'DETECT_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');
    
    -- Amount-based anomalies
    INSERT INTO ANOMALY_DETECTION_RESULTS (
        detection_id, invoice_id, anomaly_type, severity, description, statistical_metrics
    )
    WITH stats AS (
        SELECT 
            AVG(total) as avg_total,
            STDDEV(total) as stddev_total
        FROM TRANSACT_TOTALS
    ),
    outliers AS (
        SELECT 
            t.invoice_id,
            t.total,
            s.avg_total,
            s.stddev_total,
            (t.total - s.avg_total) / s.stddev_total as z_score
        FROM TRANSACT_TOTALS t
        CROSS JOIN stats s
        WHERE ABS((t.total - s.avg_total) / s.stddev_total) > 2
    )
    SELECT 
        detection_id,
        invoice_id,
        'AMOUNT_OUTLIER',
        CASE 
            WHEN ABS(z_score) > 3 THEN 'Critical'
            WHEN ABS(z_score) > 2.5 THEN 'High'
            ELSE 'Medium'
        END,
        'Invoice amount significantly deviates from normal pattern. Z-score: ' || ROUND(z_score, 2),
        OBJECT_CONSTRUCT('z_score', z_score, 'amount', total, 'avg_amount', avg_total)
    FROM outliers;
    
    GET DIAGNOSTICS anomaly_count = ROW_COUNT;
    
    -- Date-based anomalies (weekend invoices, holidays, etc.)
    INSERT INTO ANOMALY_DETECTION_RESULTS (
        detection_id, invoice_id, anomaly_type, severity, description
    )
    SELECT 
        detection_id,
        invoice_id,
        'TIMING_ANOMALY',
        'Medium',
        'Invoice dated on weekend: ' || TO_VARCHAR(invoice_date, 'Day, YYYY-MM-DD')
    FROM TRANSACT_TOTALS
    WHERE DAYOFWEEK(invoice_date) IN (1, 7); -- Sunday = 1, Saturday = 7
    
    RETURN 'Detected anomalies in batch: ' || detection_id || '. Found ' || anomaly_count || ' amount outliers.';
END;
$$;

-- ===============================================
-- ENHANCED VIEWS FOR ANALYTICS
-- ===============================================

-- Comprehensive Invoice Analytics View
CREATE OR REPLACE VIEW VW_INVOICE_ANALYTICS AS
SELECT 
    t.invoice_id,
    t.invoice_date,
    t.total,
    ic.category,
    ic.confidence_score as category_confidence,
    ira.risk_level,
    ira.risk_score,
    COUNT(ti.product_name) as item_count,
    AVG(ti.unit_price) as avg_unit_price,
    MAX(ti.unit_price) as max_unit_price,
    CASE 
        WHEN DAYOFWEEK(t.invoice_date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    CASE 
        WHEN ri.review_status = 'Auto-reconciled' AND rt.review_status = 'Auto-reconciled' THEN 'Fully Auto-Reconciled'
        WHEN ri.review_status = 'Reviewed' OR rt.review_status = 'Reviewed' THEN 'Manually Reviewed'
        ELSE 'Pending Review'
    END as reconciliation_status
FROM TRANSACT_TOTALS t
LEFT JOIN TRANSACT_ITEMS ti ON t.invoice_id = ti.invoice_id
LEFT JOIN INVOICE_CATEGORIES ic ON t.invoice_id = ic.invoice_id
LEFT JOIN INVOICE_RISK_ASSESSMENT ira ON t.invoice_id = ira.invoice_id
LEFT JOIN RECONCILE_RESULTS_ITEMS ri ON t.invoice_id = ri.invoice_id
LEFT JOIN RECONCILE_RESULTS_TOTALS rt ON t.invoice_id = rt.invoice_id
GROUP BY 
    t.invoice_id, t.invoice_date, t.total, ic.category, ic.confidence_score,
    ira.risk_level, ira.risk_score, t.invoice_date, ri.review_status, rt.review_status;

-- Monthly Spend Trends View
CREATE OR REPLACE VIEW VW_MONTHLY_SPEND_TRENDS AS
SELECT 
    DATE_TRUNC('MONTH', invoice_date) as month,
    COUNT(DISTINCT invoice_id) as invoice_count,
    SUM(total) as total_spend,
    AVG(total) as avg_invoice_amount,
    MIN(total) as min_amount,
    MAX(total) as max_amount,
    STDDEV(total) as amount_stddev,
    COUNT(DISTINCT CASE WHEN risk_level = 'High' THEN invoice_id END) as high_risk_count
FROM VW_INVOICE_ANALYTICS
GROUP BY DATE_TRUNC('MONTH', invoice_date)
ORDER BY month;

-- Category Performance View
CREATE OR REPLACE VIEW VW_CATEGORY_PERFORMANCE AS
SELECT 
    category,
    COUNT(DISTINCT invoice_id) as invoice_count,
    SUM(total) as total_spend,
    AVG(total) as avg_amount,
    AVG(category_confidence) as avg_confidence,
    COUNT(DISTINCT CASE WHEN risk_level = 'High' THEN invoice_id END) as high_risk_invoices,
    AVG(item_count) as avg_items_per_invoice
FROM VW_INVOICE_ANALYTICS
WHERE category IS NOT NULL
GROUP BY category
ORDER BY total_spend DESC;

-- ===============================================
-- AUTOMATED TASKS FOR AI PROCESSING
-- ===============================================

-- Task: Automated Fraud Risk Assessment
CREATE OR REPLACE TASK TASK_FRAUD_ASSESSMENT
    WAREHOUSE = doc_ai_qs_wh
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE_DB_STREAM')
AS
BEGIN
    CALL SP_BATCH_FRAUD_ASSESSMENT();
END;

-- Task: Automated Invoice Categorization  
CREATE OR REPLACE TASK TASK_AI_CATEGORIZATION
    WAREHOUSE = doc_ai_qs_wh
    SCHEDULE = '120 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE_DB_STREAM')
AS
BEGIN
    CALL SP_AI_CATEGORIZE_INVOICES();
END;

-- Task: Daily Anomaly Detection
CREATE OR REPLACE TASK TASK_ANOMALY_DETECTION
    WAREHOUSE = doc_ai_qs_wh
    SCHEDULE = 'USING CRON 0 9 * * * UTC' -- Daily at 9 AM UTC
AS
BEGIN
    CALL SP_DETECT_ANOMALIES();
END;

-- Resume all new tasks
ALTER TASK TASK_FRAUD_ASSESSMENT RESUME;
ALTER TASK TASK_AI_CATEGORIZATION RESUME;
ALTER TASK TASK_ANOMALY_DETECTION RESUME;

-- ===============================================
-- SAMPLE AI QUERIES AND FUNCTIONS
-- ===============================================

-- Function: Get AI-powered invoice summary
CREATE OR REPLACE FUNCTION GET_INVOICE_AI_SUMMARY(invoice_id_param VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    WITH invoice_details AS (
        SELECT 
            t.invoice_id,
            t.total,
            t.invoice_date,
            ic.category,
            ira.risk_level,
            LISTAGG(ti.product_name, ', ') as products
        FROM TRANSACT_TOTALS t
        LEFT JOIN TRANSACT_ITEMS ti ON t.invoice_id = ti.invoice_id
        LEFT JOIN INVOICE_CATEGORIES ic ON t.invoice_id = ic.invoice_id
        LEFT JOIN INVOICE_RISK_ASSESSMENT ira ON t.invoice_id = ira.invoice_id
        WHERE t.invoice_id = invoice_id_param
        GROUP BY t.invoice_id, t.total, t.invoice_date, ic.category, ira.risk_level
    )
    SELECT 
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            'Provide a concise business summary for this invoice: ' ||
            'ID=' || invoice_id || ', Amount=$' || total || ', Date=' || invoice_date ||
            ', Category=' || COALESCE(category, 'Unknown') || ', Risk=' || COALESCE(risk_level, 'Not Assessed') ||
            ', Products=' || products || '. Focus on key business insights.'
        )
    FROM invoice_details
$$;

-- ===============================================
-- INITIAL DATA POPULATION
-- ===============================================

-- Run initial fraud assessment on existing data
CALL SP_BATCH_FRAUD_ASSESSMENT();

-- Run initial categorization
CALL SP_AI_CATEGORIZE_INVOICES();

-- Run initial anomaly detection
CALL SP_DETECT_ANOMALIES();

-- ===============================================
-- SAMPLE QUERIES FOR TESTING
-- ===============================================

-- Test the enhanced analytics
SELECT * FROM VW_INVOICE_ANALYTICS LIMIT 10;

-- Check monthly trends
SELECT * FROM VW_MONTHLY_SPEND_TRENDS;

-- Review categories
SELECT * FROM VW_CATEGORY_PERFORMANCE;

-- Check anomalies
SELECT * FROM ANOMALY_DETECTION_RESULTS WHERE severity IN ('High', 'Critical');

-- Test AI summary function
SELECT GET_INVOICE_AI_SUMMARY('2001');

COMMIT; 