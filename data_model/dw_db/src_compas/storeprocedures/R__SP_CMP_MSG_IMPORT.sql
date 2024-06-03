USE SCHEMA SRC_COMPAS;


CREATE OR REPLACE PROCEDURE SP_CMP_MSG_IMPORT("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_COMPAS_SC" VARCHAR(16777216), "LZ_COMPAS_SC" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''SP_CMP_MSG_IMPORT'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''SP_CMP_MSG_IMPORT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;


V_MSG_LOG_STG          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.MSG_LOG_STG'';  
V_MSG_TXT_STG          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.MSG_TXT_STG''; 
 
V_MESSAGE_TYPES        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.MESSAGE_TYPES'';
V_FILTERED_MSGS_STG    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.FILTERED_MSGS_STG'';
V_XML_CLEANED_STG      VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.XML_CLEANED_STG'';
V_XML_FAILED_STG       VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.XML_FAILED_STG'';    

V_CLG_MSG              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.CLG_MSG''; 
V_CLG_MSG_ERRORS       VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.CLG_MSG_ERRORS''; 



BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';


V_STEP := ''STEP1'';
   
V_STEP_NAME := ''Truncating staging tables''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
TRUNCATE TABLE IDENTIFIER (:V_FILTERED_MSGS_STG);
TRUNCATE TABLE IDENTIFIER (:V_XML_CLEANED_STG);
TRUNCATE TABLE IDENTIFIER (:V_XML_FAILED_STG);
        

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);





V_STEP := ''STEP2'';
   
V_STEP_NAME := ''Filtering the log and txt table based on the lookup table''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

INSERT INTO IDENTIFIER(:V_FILTERED_MSGS_STG)
            SELECT
            vmtp.msg_type_id,
            vmtp.msg_desc,
            vmtp.inbound,
            vmtp.msg_category_id,
            vmtp.msg_type_created_by,
            vmtp.msg_type_creation_date,
            vmtp.msg_type_last_modified_by,
            vmtp.msg_type_last_modified_date,
            ml.msg_log_id,
            ml.bodid,
            ml.msg_status_id,
            ml.reference_id,
            ml.msg_log_create_date,
            ml.msg_log_created_by,
            ml.msg_log_creation_date,
            mt.msg_text_id,
            REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( mt.msg_clob, ''<\\\\?[^>]+\\\\?> '', '''' ), ''<\\\\?[^>]+\\\\?>'', '''' ), '' <[a-zA-Z0-9_-]*:'', ''<'' ), ''<[a-zA-Z0-9_-]*:'', ''<'' ), '' </[a-zA-Z0-9_-]*:'', ''</'' ), ''</[a-zA-Z0-9_-]*:'', ''</'' ), '' [A-Za-z0-9_-]*:[A-Za-z0-9_-]*=\\\\"[a-zA-Z:/.0-9 _&;\\\\#-]*\\\\"'', '''' ), ''[A-Za-z0-9_-]*:[A-Za-z0-9_-]*=\\\\"[a-zA-Z:/.0-9 _&;\\\\#-]*\\\\"'', '''' ), '' [A-Za-z0-9_-]*=\\\\"[a-zA-Z:/.0-9 _&;\\\\#-]*\\\\"'', '''' ), ''[A-Za-z0-9_-]*=\\\\"[a-zA-Z:/.0-9 _&;\\\\#-]*\\\\"'', '''' ), '' \\\\b(Name|Type)\\\\b=[a-zA-Z0-9_-]*=\\\\"[a-zA-Z:/.0-9 _&;\\\\#-]*\\\\"'', ''''), ''\\\\b(Name|Type)\\\\b=[a-zA-Z0-9_-]*=\\\\"[a-zA-Z:/.0-9 _&;\\\\#-]*\\\\"'', ''''), '' <!--[a-zA-Z0-9&:;/=@.()<>?\\\\"\\\\- ]*-->'', ''''), ''<!--[a-zA-Z0-9&:;/=@.()<>?\\\\"\\\\- ]*-->'', '''') as msg_clob
            
            FROM 
            IDENTIFIER (:V_MSG_LOG_STG) ml
            INNER JOIN IDENTIFIER (:V_MSG_TXT_STG) mt ON ml.msg_log_id = mt.msg_log_id
            INNER JOIN IDENTIFIER (:V_MESSAGE_TYPES) vmtp ON ml.msg_type_id = vmtp.msg_type_id;
        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_FILTERED_MSGS_STG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);






V_STEP := ''STEP3'';
   
 
V_STEP_NAME := ''Load xml_cleaned_stg stage table''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

INSERT INTO IDENTIFIER(:V_XML_CLEANED_STG)
            SELECT
            msg_type_id,
            msg_desc,
            inbound,
            msg_category_id,
            msg_type_created_by,
            msg_type_creation_date,
            msg_type_last_modified_by,
            msg_type_last_modified_date,
            msg_log_id,
            bodid,
            msg_status_id,
            reference_id,
            msg_log_create_date,
            msg_log_created_by,
            msg_log_creation_date,
            msg_text_id,
            PARSE_XML(msg_clob) as msg_clob
            FROM IDENTIFIER(:V_FILTERED_MSGS_STG)
            WHERE 
            CHECK_XML(msg_clob) IS NULL;
    

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_XML_CLEANED_STG)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


                                               
                                 
                                 
V_STEP := ''STEP4'';
   
V_STEP_NAME := ''Loading xml_failed_stg stage table''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
INSERT INTO IDENTIFIER(:V_XML_FAILED_STG)
            SELECT
            ''XMLSyntaxError'' as error_desc,
            msg_type_id,
            msg_desc,
            inbound,
            msg_category_id,
            msg_type_created_by,
            msg_type_creation_date,
            msg_type_last_modified_by,
            msg_type_last_modified_date,
            msg_log_id,
            bodid,
            msg_status_id,
            reference_id,
            msg_log_create_date,
            msg_log_created_by,
            msg_log_creation_date,
            msg_text_id,
            msg_clob
            FROM IDENTIFIER(:V_FILTERED_MSGS_STG)
            WHERE 
            CHECK_XML(msg_clob) IS NOT NULL;  
        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_XML_FAILED_STG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);                                
                                 
                                 
                                 
                                 
V_STEP := ''STEP5'';
   
V_STEP_NAME := ''INSERTING CLEANED XML TO clg_msg TABLE''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
INSERT INTO IDENTIFIER (:V_CLG_MSG) 
(
msg_desc,
inbound,
msg_category_id,
msg_type_created_by,
msg_type_creation_date,
msg_type_last_modified_by,
msg_type_last_modified_date,
msg_log_id,
bodid,
msg_status_id,
reference_id,
msg_log_create_date,
msg_log_created_by,
msg_log_creation_date,
msg_text_id,
msg_clob,
msg_log_creation_date_dl,
msg_type_id

)
SELECT
msg_desc,
inbound,
msg_category_id,
msg_type_created_by,
msg_type_creation_date,
msg_type_last_modified_by,
msg_type_last_modified_date,
msg_log_id,
bodid,
msg_status_id,
reference_id,
msg_log_create_date,
msg_log_created_by,
msg_log_creation_date,
msg_text_id,
msg_clob,
TO_VARCHAR(msg_log_creation_date,''yyyy-MM-dd'') AS msg_log_creation_date_dl,
msg_type_id
FROM IDENTIFIER(:V_XML_CLEANED_STG);
        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_CLG_MSG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);                               
                                 
                                 
                                 
                                 
V_STEP := ''STEP6'';
   
V_STEP_NAME := ''INSERTING FAILED XML TO clg_msg_errors TABLE''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
INSERT INTO IDENTIFIER (:V_CLG_MSG_ERRORS) 
(
error_desc,
msg_desc,
inbound,
msg_category_id,
msg_type_created_by,
msg_type_creation_date,
msg_type_last_modified_by,
msg_type_last_modified_date,
msg_log_id,
bodid,
msg_status_id,
reference_id,
msg_log_create_date,
msg_log_created_by,
msg_log_creation_date,
msg_text_id,
msg_clob,
msg_log_creation_date_dl,
msg_type_id

)
SELECT
error_desc,
msg_desc,
inbound,
msg_category_id,
msg_type_created_by,
msg_type_creation_date,
msg_type_last_modified_by,
msg_type_last_modified_date,
msg_log_id,
bodid,
msg_status_id,
reference_id,
msg_log_create_date,
msg_log_created_by,
msg_log_creation_date,
msg_text_id,
msg_clob,
TO_VARCHAR(msg_log_creation_date,''yyyy-MM-dd'') AS msg_log_creation_date_dl,
msg_type_id
FROM IDENTIFIER(:V_XML_FAILED_STG);
        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_CLG_MSG_ERRORS));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);               
                                 




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
