# 📋 DocAI Validation System - Deployment Guide

This guide covers the complete setup and deployment of the DocAI Invoice Validation System.

## 🎯 Overview

The DocAI Validation System enables human validators to:
- **Review PDF invoices** with embedded viewer
- **Validate DocAI extractions** with edit capabilities  
- **Track changes** and maintain audit trails
- **Monitor validation progress** with dashboard
- **Store validated data** in silver layer tables

## 📋 Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   INVOICE_INFO  │───▶│  Validation App │───▶│ SILVER_VALIDATED│
│   (Bronze)      │    │   (Streamlit)   │    │   (Silver)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐              │
         └─────────────▶│   PDF STAGE     │◀─────────────┘
                        │  (File Storage) │
                        └─────────────────┘
```

## 🚀 Step 1: SQL Setup

### Run the Validation Setup Script

```sql
-- Execute the validation_setup.sql file
-- This creates all necessary tables, views, and procedures
```

**Key Objects Created:**

### Tables
- **`SILVER_VALIDATED_INVOICES`** - Validated invoice data with audit trail
- **`VW_PENDING_VALIDATIONS`** - View of invoices awaiting validation  
- **`VW_VALIDATION_STATS`** - Validation progress statistics

### Procedures  
- **`SP_VALIDATE_INVOICE()`** - Core validation logic with change tracking
- **`GET_PDF_PRESIGNED_URL()`** - PDF URL generation for viewer

### Permissions
- All necessary grants for `doc_ai_qs_role`

## 🎨 Step 2: Deploy Streamlit App

### Option A: Streamlit in Snowflake (Recommended)

1. **Upload the App:**
   ```sql
   -- In Snowsight, go to Streamlit Apps
   -- Create New App
   -- Name: "DocAI Invoice Validation"
   -- Upload: sis_docai_validation_app.py
   ```

2. **Configure Permissions:**
   ```sql
   -- Grant Streamlit app access to required objects
   GRANT USAGE ON DATABASE doc_ai_qs_db TO APPLICATION ROLE docai_validation_app;
   GRANT USAGE ON SCHEMA doc_ai_qs_db.doc_ai_schema TO APPLICATION ROLE docai_validation_app;
   GRANT SELECT ON ALL TABLES IN SCHEMA doc_ai_qs_db.doc_ai_schema TO APPLICATION ROLE docai_validation_app;
   GRANT SELECT ON ALL VIEWS IN SCHEMA doc_ai_qs_db.doc_ai_schema TO APPLICATION ROLE docai_validation_app;
   ```

### Option B: Local Development

1. **Setup Environment:**
   ```bash
   conda activate doc-ai-env  # Use existing conda environment
   pip install streamlit
   ```

2. **Run Locally:**
   ```bash
   streamlit run sis_docai_validation_app.py
   ```

## 📊 Step 3: Test the System

### 1. Verify Data Setup
```sql
-- Check if invoices are available for validation
SELECT COUNT(*) FROM doc_ai_qs_db.doc_ai_schema.INVOICE_INFO;

-- View pending validations
SELECT * FROM doc_ai_qs_db.doc_ai_schema.VW_PENDING_VALIDATIONS LIMIT 5;

-- Check validation statistics
SELECT * FROM doc_ai_qs_db.doc_ai_schema.VW_VALIDATION_STATS;
```

### 2. Test PDF Access
```sql
-- Test PDF URL generation
SELECT doc_ai_qs_db.doc_ai_schema.GET_PDF_PRESIGNED_URL('Custom_Invoice_2001.pdf');
```

### 3. Test Validation Procedure
```sql
-- Test validation with changes
CALL doc_ai_qs_db.doc_ai_schema.SP_VALIDATE_INVOICE(
    '2001',                    -- Invoice No
    'Custom_Invoice_2001.pdf', -- File Name  
    'Test User',               -- Validated By
    'CUST999',                 -- New Customer No
    NULL,                      -- New Invoice Date (no change)
    NULL,                      -- New Total Amount (no change)
    NULL,                      -- New Cost Center (no change)
    'Test validation',         -- Notes
    'VALIDATED'                -- Status
);

-- Check result
SELECT * FROM doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES 
WHERE invoice_no = '2001';
```

## 🎯 Step 4: Using the Validation App

### 📋 Validation Queue Tab

1. **Filter Invoices:**
   - Status filter (All/Pending/Validated/Rejected)
   - Date filter for recent invoices
   - Search by invoice number

2. **Select Invoice:**
   - Choose from dropdown list
   - View PDF preview on left side
   - Edit extracted values on right side

3. **Validation Actions:**
   - **✅ Validate & Approve** - Mark as validated
   - **❌ Reject** - Mark as rejected with notes
   - **💾 Save as Draft** - Save changes without finalizing

### 📈 Dashboard Tab

- **Progress Metrics** - Total, validated, pending counts
- **Validation Rate** - Percentage completion
- **Recent Activity** - Last 10 validations
- **Corrections Tracking** - Changes made during validation

## 🔧 Troubleshooting

### Common Issues

#### 1. PDF Preview Not Working
```sql
-- Check stage access and file existence
LIST @doc_ai_qs_db.doc_ai_schema.doc_ai_stage;

-- Test presigned URL generation
SELECT GET_PRESIGNED_URL('@doc_ai_qs_db.doc_ai_schema.doc_ai_stage', 'your_file.pdf', 3600);
```

#### 2. Connection Errors in Streamlit
- Ensure `st.connection("snowflake")` is properly configured
- Check Snowflake connection parameters
- Verify role permissions

#### 3. No Invoices Showing
```sql
-- Check if INVOICE_INFO has data
SELECT COUNT(*) FROM doc_ai_qs_db.doc_ai_schema.INVOICE_INFO;

-- Check view definition
SELECT * FROM doc_ai_qs_db.doc_ai_schema.VW_PENDING_VALIDATIONS LIMIT 1;
```

#### 4. Validation Procedure Errors
```sql
-- Check procedure exists
SHOW PROCEDURES LIKE '%VALIDATE_INVOICE%';

-- Test with minimal parameters
CALL doc_ai_qs_db.doc_ai_schema.SP_VALIDATE_INVOICE('TEST', 'test.pdf', 'User', NULL, NULL, NULL, NULL, NULL, 'VALIDATED');
```

### Performance Tips

1. **Large Document Sets:**
   - Use filters to limit displayed invoices
   - Consider pagination for 1000+ invoices

2. **PDF Loading:**
   - PDFs load on-demand when selected
   - Consider smaller file sizes if loading is slow

3. **Database Performance:**
   - Views include indexes on commonly filtered columns
   - Validation stats are computed efficiently

## 📋 Validation Workflow

### Typical User Journey

1. **Login** → Enter validator name in sidebar
2. **Review Stats** → Check pending validation count
3. **Select Invoice** → Choose from filtered list
4. **View PDF** → Review original document  
5. **Edit Fields** → Correct any DocAI extraction errors
6. **Add Notes** → Document validation decisions
7. **Submit** → Validate, reject, or save as draft
8. **Next Invoice** → Continue with remaining invoices

### Data Flow

```
INVOICE_INFO → VW_PENDING_VALIDATIONS → Streamlit App → SP_VALIDATE_INVOICE → SILVER_VALIDATED_INVOICES
```

## 🎯 Advanced Features

### Custom Validation Rules
Add business logic to `SP_VALIDATE_INVOICE` for:
- Required field validation
- Format checking (dates, amounts)
- Business rule enforcement

### Batch Operations
Extend with bulk validation capabilities:
- Approve multiple invoices
- Bulk reject with common reasons
- Export validation reports

### Integration Points
- **Workflow Systems** - Trigger validations from external systems
- **Analytics** - Export validation metrics
- **Notifications** - Alert on validation milestones

## 📊 Monitoring & Metrics

### Key Metrics to Track
- **Validation Rate** - % of invoices validated
- **Error Rate** - % requiring corrections  
- **Processing Time** - Average time per validation
- **User Performance** - Validations per user

### SQL Queries for Monitoring
```sql
-- Daily validation progress
SELECT 
    DATE(validated_timestamp) as validation_date,
    COUNT(*) as validations_completed,
    COUNT(CASE WHEN changes_made THEN 1 END) as corrections_made
FROM doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES
WHERE validated_timestamp >= CURRENT_DATE - 7
GROUP BY DATE(validated_timestamp)
ORDER BY validation_date DESC;

-- User productivity
SELECT 
    validated_by,
    COUNT(*) as total_validations,
    COUNT(CASE WHEN changes_made THEN 1 END) as corrections_made,
    AVG(CASE WHEN changes_made THEN 1 ELSE 0 END) as correction_rate
FROM doc_ai_qs_db.doc_ai_schema.SILVER_VALIDATED_INVOICES
WHERE validation_status = 'VALIDATED'
GROUP BY validated_by
ORDER BY total_validations DESC;
```

## ✅ Success Criteria

Your DocAI Validation System is successfully deployed when:

- ✅ **SQL objects created** without errors
- ✅ **Streamlit app loads** and connects to Snowflake  
- ✅ **PDF previews display** correctly
- ✅ **Validation workflow** completes end-to-end
- ✅ **Audit trail captured** in silver layer
- ✅ **Dashboard shows metrics** accurately

🎉 **Congratulations!** Your DocAI Validation System is ready for production use. 