USE ROLE ACCOUNTADMIN;

-- CREATE A DOC AI ROLE TO BE USED FOR THE QUICKSTART
CREATE ROLE doc_ai_qs_role;
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE doc_ai_qs_role;
GRANT EXECUTE TASK on account to role doc_ai_qs_role;

GRANT ROLE doc_ai_qs_role TO USER EDENDULK_SFC;
GRANT ROLE doc_ai_qs_role TO USER GAURAVKAPOOR;

-- CREATE A WAREHOUSE TO BE USED
CREATE WAREHOUSE doc_ai_qs_wh;

-- GIVE THE doc_ai_qs_role ROLE ACCESS TO THE WAREHOUSE
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE doc_ai_qs_wh TO ROLE doc_ai_qs_role;

-- CREATE DATABASE AND SCHEMA TO BE USED, GIVE THE doc_ai_qs_role ACCESS
CREATE DATABASE doc_ai_qs_db;
GRANT CREATE SCHEMA, MODIFY, USAGE ON DATABASE doc_ai_qs_db TO ROLE doc_ai_qs_role;

-- CHANGE TO THE QUICKSTART ROLE
USE ROLE doc_ai_qs_role;

-- CREATE A SCHEMA FOR THE DOCUEMNT AI MODEL, STAGE etc
CREATE SCHEMA doc_ai_qs_db.doc_ai_schema;
-- EXPLICIT GRANT USAGE AND snowflake.ml.document_intelligence on the  SCHEMA
GRANT USAGE ON SCHEMA doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE table ON SCHEMA doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE snowflake.ml.document_intelligence on schema doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE MODEL ON SCHEMA doc_ai_qs_db.doc_ai_schema TO ROLE doc_ai_qs_role;

-- CREATE A STAGE FOR STORING DOCUMENTS
CREATE OR REPLACE STAGE doc_ai_qs_db.doc_ai_schema.doc_ai_stage
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

USE SCHEMA doc_ai_qs_db.doc_ai_schema;


-- CREATE A STREAM TO MONITOR THE STAGE FOR NEW FILE UPLOADS
CREATE OR REPLACE STREAM doc_ai_qs_db.doc_ai_schema.INVOICE_STREAM 
ON DIRECTORY(@DOC_AI_STAGE);

CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.INVOICE_INFO (
    INVOICE_NO VARCHAR(255),
    CUSTOMER_NO VARCHAR(255),
    INVOICE_DATE VARCHAR(255),
    TOTAL_AMOUNT NUMBER(12, 2),
    COST_CENTER VARCHAR(255),
    file_name VARCHAR(255),
    file_size NUMBER(12, 2),
    last_modified TIMESTAMP_TZ,
    snowflake_file_url VARCHAR(255)
);


-- CREATE A TASK TO RUN WHEN THE STREAM DETECTS NEW FILE UPLOADS IN OUR STAGE
create or replace task doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT
	warehouse=doc_ai_qs_wh
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('INVOICE_STREAM')
	as BEGIN
        CREATE OR REPLACE TEMPORARY TABLE doc_ai_qs_db.doc_ai_schema.docai_parsed AS (
        SELECT
            Relative_path as file_name,
            size as file_size,
            last_modified,
            file_url as snowflake_file_url,
            PARSE_JSON(DOC_AI_QS_DB.DOC_AI_SCHEMA.DOCAI_INVOICE_ITEMS!PREDICT(get_presigned_url('@doc_ai_stage', RELATIVE_PATH ), 1)) AS json_data
        FROM doc_ai_qs_db.doc_ai_schema.INVOICE_STREAM
        WHERE METADATA$ACTION = 'INSERT' OR METADATA$ACTION = 'UPDATE'
        );

        CREATE OR REPLACE TEMPORARY TABLE extracted_item_data AS (
        SELECT
            -- Extract values using the correct JSON paths from Document AI
            p.json_data:"INVOICE_INFO|INVOICE_NO"[0].value::STRING AS INVOICE_NO,
            p.json_data:"INVOICE_INFO|CUSTOMER_NO"[0].value::STRING AS CUSTOMER_NO,
            p.json_data:"INVOICE_INFO|INVOICE_DATE"[0].value::STRING AS INVOICE_DATE,
            -- Parse European number format (comma as decimal, period as thousands)
            TRY_CAST(
                CASE 
                    -- Handle European format with both periods and commas (e.g., "1.234.567,89")
                    WHEN REGEXP_COUNT(REPLACE(REPLACE(p.json_data:"INVOICE_INFO|TOTAL_AMOUNT"[0].value::STRING, 'USD ', ''), '$', ''), '\\.') > 0 
                         AND REGEXP_COUNT(REPLACE(REPLACE(p.json_data:"INVOICE_INFO|TOTAL_AMOUNT"[0].value::STRING, 'USD ', ''), '$', ''), ',') > 0 THEN
                        REPLACE(
                            REPLACE(
                                REPLACE(REPLACE(p.json_data:"INVOICE_INFO|TOTAL_AMOUNT"[0].value::STRING, 'USD ', ''), '$', ''),
                                '.', ''  -- Remove all periods (thousands separators in European format)
                            ),
                            ',', '.'  -- Convert comma decimal separator to period
                        )
                    -- Handle European format with only comma (e.g., "811,60")
                    WHEN REGEXP_COUNT(REPLACE(REPLACE(p.json_data:"INVOICE_INFO|TOTAL_AMOUNT"[0].value::STRING, 'USD ', ''), '$', ''), ',') = 1 
                         AND REGEXP_COUNT(REPLACE(REPLACE(p.json_data:"INVOICE_INFO|TOTAL_AMOUNT"[0].value::STRING, 'USD ', ''), '$', ''), '\\.') = 0
                         AND RIGHT(REPLACE(REPLACE(p.json_data:"INVOICE_INFO|TOTAL_AMOUNT"[0].value::STRING, 'USD ', ''), '$', ''), 3) LIKE ',%' THEN
                        REPLACE(
                            REPLACE(REPLACE(p.json_data:"INVOICE_INFO|TOTAL_AMOUNT"[0].value::STRING, 'USD ', ''), '$', ''),
                            ',', '.'  -- Convert comma decimal separator to period
                        )
                    -- Handle US format or integers with comma thousands separators (e.g., "1,234" or "1,234.56")
                    ELSE
                        REPLACE(
                            REPLACE(REPLACE(p.json_data:"INVOICE_INFO|TOTAL_AMOUNT"[0].value::STRING, 'USD ', ''), '$', ''),
                            ',', ''  -- Remove comma thousands separators
                        )
                END
            AS NUMBER(12, 2)) AS TOTAL_AMOUNT,
            p.json_data:"INVOICE_INFO|COST_CENTER"[0].value::STRING AS COST_CENTER,
            file_name,
            file_size,
            last_modified,
            snowflake_file_url
        FROM
            doc_ai_qs_db.doc_ai_schema.docai_parsed p
        );
        
        DELETE FROM doc_ai_qs_db.doc_ai_schema.INVOICE_INFO
        WHERE invoice_no IN (SELECT DISTINCT invoice_no FROM extracted_item_data);
        
        -- Step 2: Insert all the new line items from incoming_data.
        INSERT INTO doc_ai_qs_db.doc_ai_schema.INVOICE_INFO (
            INVOICE_NO,
            CUSTOMER_NO,
            INVOICE_DATE,
            TOTAL_AMOUNT,
            COST_CENTER,
            file_name,
            file_size,
            last_modified,
            snowflake_file_url)
        SELECT
            INVOICE_NO,
            CUSTOMER_NO,
            INVOICE_DATE,
            TOTAL_AMOUNT,
            COST_CENTER,
            file_name,
            file_size,
            last_modified,
            snowflake_file_url
        FROM extracted_item_data;
    END;

ALTER TASK doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT RESUME;


