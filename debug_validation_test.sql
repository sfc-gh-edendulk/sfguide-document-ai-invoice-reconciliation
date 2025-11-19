-- ===============================================
-- Debug Validation System - Manual Testing
-- ===============================================

USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

-- Step 1: Check if we have data in INVOICE_INFO table
SELECT 'INVOICE_INFO Data:' as step;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.INVOICE_INFO LIMIT 5;

-- Step 2: Check the pending validations view
SELECT 'PENDING_VALIDATIONS View:' as step;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.VW_PENDING_VALIDATIONS LIMIT 5;

-- Step 3: Check current SILVER_VALIDATED_INVOICES table
SELECT 'SILVER_VALIDATED_INVOICES Before Test:' as step;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES;

-- Step 4: Test the stored procedure manually
-- Replace these values with actual data from your INVOICE_INFO table
SELECT 'Testing Stored Procedure:' as step;
CALL doc_ai_qs_db.doc_ai_schema.SP_VALIDATE_INVOICE(
    '124100',           -- P_INVOICE_NO (use actual invoice from your data)
    '124100-.pdf',      -- P_FILE_NAME (use actual filename)
    'TEST_USER',        -- P_VALIDATED_BY
    'NEW_CUSTOMER',     -- P_NEW_CUSTOMER_NO (test change)
    NULL,               -- P_NEW_INVOICE_DATE (no change)
    NULL,               -- P_NEW_TOTAL_AMOUNT (no change)
    NULL,               -- P_NEW_COST_CENTER (no change)
    'Manual test validation',  -- P_VALIDATION_NOTES
    'VALIDATED'         -- P_VALIDATION_STATUS
);

-- Step 5: Check SILVER_VALIDATED_INVOICES after procedure call
SELECT 'SILVER_VALIDATED_INVOICES After Test:' as step;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES;

-- Step 6: Check the view again to see if status changed
SELECT 'PENDING_VALIDATIONS After Test:' as step;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.VW_PENDING_VALIDATIONS 
WHERE invoice_no = '124100';

-- Step 7: Check validation stats
SELECT 'VALIDATION_STATS:' as step;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.VW_VALIDATION_STATS;

-- Step 8: Test procedure with no changes (should still work)
SELECT 'Testing Procedure with No Changes:' as step;
CALL doc_ai_qs_db.doc_ai_schema.SP_VALIDATE_INVOICE(
    '124100',           -- Use same invoice
    '124100-.pdf',      -- Same file
    'TEST_USER_2',      -- Different user
    NULL,               -- No customer change
    NULL,               -- No date change  
    NULL,               -- No amount change
    NULL,               -- No cost center change
    'Test with no changes',  -- Notes
    'VALIDATED'         -- Status
);

-- Step 9: Final check
SELECT 'Final Results:' as step;
SELECT 
    invoice_no,
    validation_status,
    changes_made,
    change_summary,
    validated_by,
    validation_notes
FROM doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES; 