-- ===============================================
-- DocAI Validation System Setup
-- Silver Layer Tables and Supporting Objects
-- ===============================================

USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

-- ===============================================
-- Silver Layer: Validated Invoice Information
-- ===============================================

-- Create silver layer table for validated invoice data
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES (
    -- Original DocAI extracted fields
    invoice_no VARCHAR(255),
    customer_no VARCHAR(255),
    invoice_date VARCHAR(255),
    total_amount NUMBER(12, 2),
    cost_center VARCHAR(255),
    
    -- File metadata
    file_name VARCHAR(255),
    file_size NUMBER(12, 2),
    last_modified TIMESTAMP_TZ,
    snowflake_file_url VARCHAR(255),
    
    -- Validation metadata
    validation_status VARCHAR(50) DEFAULT 'PENDING', -- PENDING, VALIDATED, REJECTED
    validated_by VARCHAR(100),
    validated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Change tracking
    changes_made BOOLEAN DEFAULT FALSE,
    original_values VARIANT, -- JSON of original values before changes
    change_summary VARCHAR(1000), -- Human-readable summary of changes
    validation_notes VARCHAR(2000), -- User comments/notes
    
    -- Audit fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===============================================
-- Views for Validation Interface
-- ===============================================

-- View showing pending validations with file URLs
CREATE OR REPLACE VIEW doc_ai_qs_db.doc_ai_schema.VW_PENDING_VALIDATIONS AS
SELECT 
    i.invoice_no,
    i.customer_no,
    i.invoice_date,
    i.total_amount,
    i.cost_center,
    i.file_name,
    i.file_size,
    i.last_modified,
    i.snowflake_file_url,
    -- Check if already validated
    CASE 
        WHEN sv.invoice_no IS NOT NULL THEN sv.validation_status
        ELSE 'PENDING'
    END as current_status,
    sv.validated_by,
    sv.validated_timestamp,
    sv.change_summary
FROM doc_ai_qs_db.doc_ai_schema.INVOICE_INFO i
LEFT JOIN doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES sv 
    ON i.invoice_no = sv.invoice_no AND i.file_name = sv.file_name
ORDER BY i.last_modified DESC;

-- View showing validation statistics
CREATE OR REPLACE VIEW doc_ai_qs_db.doc_ai_schema.VW_VALIDATION_STATS AS
SELECT 
    COUNT(*) as total_invoices,
    COUNT(CASE WHEN sv.validation_status = 'VALIDATED' THEN 1 END) as validated_count,
    COUNT(CASE WHEN sv.validation_status = 'REJECTED' THEN 1 END) as rejected_count,
    COUNT(CASE WHEN sv.validation_status IS NULL THEN 1 END) as pending_count,
    ROUND(COUNT(CASE WHEN sv.validation_status = 'VALIDATED' THEN 1 END) * 100.0 / COUNT(*), 2) as validation_rate,
    COUNT(CASE WHEN sv.changes_made = TRUE THEN 1 END) as corrections_made
FROM doc_ai_qs_db.doc_ai_schema.INVOICE_INFO i
LEFT JOIN doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES sv 
    ON i.invoice_no = sv.invoice_no AND i.file_name = sv.file_name;

-- ===============================================
-- Stored Procedures for Validation Operations
-- ===============================================

-- Procedure to validate an invoice with optional changes (Direct SQL approach)
CREATE OR REPLACE PROCEDURE doc_ai_qs_db.doc_ai_schema.SP_VALIDATE_INVOICE(
    P_INVOICE_NO VARCHAR,
    P_FILE_NAME VARCHAR,
    P_VALIDATED_BY VARCHAR,
    P_NEW_CUSTOMER_NO VARCHAR DEFAULT NULL,
    P_NEW_INVOICE_DATE VARCHAR DEFAULT NULL,
    P_NEW_TOTAL_AMOUNT NUMBER DEFAULT NULL,
    P_NEW_COST_CENTER VARCHAR DEFAULT NULL,
    P_VALIDATION_NOTES VARCHAR DEFAULT NULL,
    P_VALIDATION_STATUS VARCHAR DEFAULT 'VALIDATED'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Delete existing record if it exists (simpler than update logic)
    DELETE FROM doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES
    WHERE invoice_no = :P_INVOICE_NO AND file_name = :P_FILE_NAME;
    
    -- Insert new validation record with all logic in SQL
    INSERT INTO doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES (
        invoice_no, customer_no, invoice_date, total_amount, cost_center,
        file_name, file_size, last_modified, snowflake_file_url,
        validation_status, validated_by, validated_timestamp,
        changes_made, original_values, change_summary, validation_notes
    )
    SELECT 
        i.invoice_no,
        COALESCE(:P_NEW_CUSTOMER_NO, i.customer_no) as customer_no,
        COALESCE(:P_NEW_INVOICE_DATE, i.invoice_date) as invoice_date,
        COALESCE(:P_NEW_TOTAL_AMOUNT, i.total_amount) as total_amount,
        COALESCE(:P_NEW_COST_CENTER, i.cost_center) as cost_center,
        i.file_name,
        i.file_size,
        i.last_modified,
        i.snowflake_file_url,
        :P_VALIDATION_STATUS,
        :P_VALIDATED_BY,
        CURRENT_TIMESTAMP(),
        -- Determine if changes were made
        CASE WHEN (
            (:P_NEW_CUSTOMER_NO IS NOT NULL AND :P_NEW_CUSTOMER_NO != i.customer_no) OR
            (:P_NEW_INVOICE_DATE IS NOT NULL AND :P_NEW_INVOICE_DATE != i.invoice_date) OR
            (:P_NEW_TOTAL_AMOUNT IS NOT NULL AND :P_NEW_TOTAL_AMOUNT != i.total_amount) OR
            (:P_NEW_COST_CENTER IS NOT NULL AND :P_NEW_COST_CENTER != i.cost_center)
        ) THEN TRUE ELSE FALSE END as changes_made,
        -- Create original values object
        OBJECT_CONSTRUCT(
            'customer_no', i.customer_no,
            'invoice_date', i.invoice_date,
            'total_amount', i.total_amount,
            'cost_center', i.cost_center
        ) as original_values,
        -- Build change summary
        CASE WHEN (
            (:P_NEW_CUSTOMER_NO IS NOT NULL AND :P_NEW_CUSTOMER_NO != i.customer_no) OR
            (:P_NEW_INVOICE_DATE IS NOT NULL AND :P_NEW_INVOICE_DATE != i.invoice_date) OR
            (:P_NEW_TOTAL_AMOUNT IS NOT NULL AND :P_NEW_TOTAL_AMOUNT != i.total_amount) OR
            (:P_NEW_COST_CENTER IS NOT NULL AND :P_NEW_COST_CENTER != i.cost_center)
        ) THEN 
            TRIM(
                COALESCE(
                    CASE WHEN :P_NEW_CUSTOMER_NO IS NOT NULL AND :P_NEW_CUSTOMER_NO != i.customer_no 
                         THEN 'Customer No: ' || COALESCE(i.customer_no, 'NULL') || ' → ' || :P_NEW_CUSTOMER_NO || '; '
                         ELSE '' END, ''
                ) ||
                COALESCE(
                    CASE WHEN :P_NEW_INVOICE_DATE IS NOT NULL AND :P_NEW_INVOICE_DATE != i.invoice_date 
                         THEN 'Invoice Date: ' || COALESCE(i.invoice_date, 'NULL') || ' → ' || :P_NEW_INVOICE_DATE || '; '
                         ELSE '' END, ''
                ) ||
                COALESCE(
                    CASE WHEN :P_NEW_TOTAL_AMOUNT IS NOT NULL AND :P_NEW_TOTAL_AMOUNT != i.total_amount 
                         THEN 'Total Amount: ' || COALESCE(i.total_amount::VARCHAR, 'NULL') || ' → ' || :P_NEW_TOTAL_AMOUNT::VARCHAR || '; '
                         ELSE '' END, ''
                ) ||
                COALESCE(
                    CASE WHEN :P_NEW_COST_CENTER IS NOT NULL AND :P_NEW_COST_CENTER != i.cost_center 
                         THEN 'Cost Center: ' || COALESCE(i.cost_center, 'NULL') || ' → ' || :P_NEW_COST_CENTER || '; '
                         ELSE '' END, ''
                )
            )
        ELSE 'No changes made' END as change_summary,
        :P_VALIDATION_NOTES
    FROM doc_ai_qs_db.doc_ai_schema.INVOICE_INFO i
    WHERE i.invoice_no = :P_INVOICE_NO AND i.file_name = :P_FILE_NAME;
    
    RETURN 'Invoice ' || :P_INVOICE_NO || ' validated successfully';
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error validating invoice: ' || SQLERRM;
END;
$$;

-- ===============================================
-- Helper Functions
-- ===============================================

-- Function to get presigned URL for PDF viewing
CREATE OR REPLACE FUNCTION doc_ai_qs_db.doc_ai_schema.GET_PDF_PRESIGNED_URL(file_name VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT GET_PRESIGNED_URL('@doc_ai_qs_db.doc_ai_schema.doc_ai_stage', file_name, 3600)
$$;

-- ===============================================
-- Sample Data and Testing
-- ===============================================

-- Grant necessary permissions
GRANT SELECT, INSERT, UPDATE ON doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES TO ROLE doc_ai_qs_role;
GRANT SELECT ON doc_ai_qs_db.doc_ai_schema.VW_PENDING_VALIDATIONS TO ROLE doc_ai_qs_role;
GRANT SELECT ON doc_ai_qs_db.doc_ai_schema.VW_VALIDATION_STATS TO ROLE doc_ai_qs_role;
GRANT USAGE ON PROCEDURE doc_ai_qs_db.doc_ai_schema.SP_VALIDATE_INVOICE(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, NUMBER, VARCHAR, VARCHAR, VARCHAR) TO ROLE doc_ai_qs_role;
GRANT USAGE ON FUNCTION doc_ai_qs_db.doc_ai_schema.GET_PDF_PRESIGNED_URL(VARCHAR) TO ROLE doc_ai_qs_role;

-- Test the validation view
SELECT * FROM doc_ai_qs_db.doc_ai_schema.VW_PENDING_VALIDATIONS LIMIT 5;

-- Test validation stats
SELECT * FROM doc_ai_qs_db.doc_ai_schema.VW_VALIDATION_STATS; 