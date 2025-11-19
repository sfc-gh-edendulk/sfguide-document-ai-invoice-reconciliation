-- ===============================================
-- Clear All Files from DOC_AI_STAGE
-- ===============================================

USE ROLE DOC_AI_QS_ROLE;
USE WAREHOUSE DOC_AI_QS_WH;
USE DATABASE DOC_AI_QS_DB;
USE SCHEMA DOC_AI_SCHEMA;

-- List files in stage before deletion (optional - for verification)
LIST @DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE;

-- Remove all files from the stage
REMOVE @DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE;

-- Refresh the stage to update the directory table
ALTER STAGE DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE REFRESH;

-- Verify stage is empty
LIST @DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE; 