use schema SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_LEGACY_CLG_MSG("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216),
                                              "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), 
                                              "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_CLG_MSG'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/msg_log_creation_date_dl=''||SUBSTR(:TRAN_MTH,1,4)||''-''||SUBSTR(:TRAN_MTH,5,2);

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_CLG_MSG''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''CLG_MSG'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''CLG_MSG'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''clg_msg''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS 
(SELECT * FROM ''||V_LZ_TBL||'' WHERE 1 = 2); '';  
 
V_QUERY2 := '' 

COPY INTO ''||:V_TEMP_TBL||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
,$9 
,$10 
,$11 
,$12
,$13 
,$14
,$15
,$16
,metadata$filename
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_TILDA_CSV'''', pattern=''''.*msg_log_creation_date_dl=.*.000.*'''' ;''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 


INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
   msg_desc , 
  inbound , 
  msg_category_id , 
  msg_type_created_by , 
  msg_type_creation_date , 
  msg_type_last_modified_by , 
  msg_type_last_modified_date , 
  msg_log_id , 
  bodid , 
  msg_status_id , 
  reference_id , 
  msg_log_create_date , 
  msg_log_created_by , 
  msg_log_creation_date , 
  msg_text_id , 
  msg_clob ,
  msg_log_creation_date_dl , 
  msg_type_id,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  msg_desc , 
  inbound , 
  msg_category_id , 
  msg_type_created_by , 
  msg_type_creation_date , 
  msg_type_last_modified_by , 
  msg_type_last_modified_date , 
  msg_log_id , 
  bodid , 
  msg_status_id , 
  reference_id , 
  msg_log_create_date , 
  msg_log_created_by , 
  msg_log_creation_date , 
  msg_text_id , 
  parse_xml(msg_clob) ,

  
  SUBSTR(msg_log_creation_date_dl, REGEXP_INSTR(msg_log_creation_date_dl, ''msg_log_creation_date_dl='')+25, 10) AS msg_log_creation_date_dl,
  SUBSTR(msg_type_id, REGEXP_INSTR(msg_type_id, ''msg_type_id='')+12, 6) AS msg_type_id,
  

CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :TRAN_MTH, NULL, NULL);

UPDATE UTIL.LEGACY_LOAD SET LOAD_INDC = ''Y'' WHERE TABLE_NAME = :V_SUB_PROCESS_NAME AND TRAN_MTH = :TRAN_MTH;

EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
 
