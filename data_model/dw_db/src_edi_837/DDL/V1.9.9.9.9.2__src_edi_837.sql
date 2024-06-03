USE SCHEMA LZ_EDI_837;

create or replace TABLE INST_CLAIM_EXTRACT_ERROR (
	CLAIM_RECORD_TYPE_HEADER VARCHAR,
	CLAIM_RECORD_TYPE_TRAILER VARCHAR,
	CLAIM_NUMBER VARCHAR(16777216),
	GRP_CONTROL_NO VARCHAR,
	TRANCACTSET_CNTL_NO VARCHAR,
	TRANSACTSET_CREATE_DATE NUMBER(38,0),
	TYPE_OF_BILL VARCHAR(16777216),
	DISCHARGE_STATUS VARCHAR(16777216),
	DRG_CODE VARCHAR(16777216),
	MEDICARE_PAID_AMOUNT VARCHAR,
	PRIMARY_DIAGNOSIS_CODE VARCHAR(16777216),
	DIAGNOSIS_CODE_2 VARCHAR(16777216),
	DIAGNOSIS_CODE_3 VARCHAR(16777216),
	DIAGNOSIS_CODE_4 VARCHAR(16777216),
	DIAGNOSIS_CODE_5 VARCHAR(16777216),
	DIAGNOSIS_CODE_6 VARCHAR(16777216),
	DIAGNOSIS_CODE_7 VARCHAR(16777216),
	DIAGNOSIS_CODE_8 VARCHAR(16777216),
	DIAGNOSIS_CODE_9 VARCHAR(16777216),
	DIAGNOSIS_CODE_10 VARCHAR(16777216),
	DIAGNOSIS_CODE_11 VARCHAR(16777216),
	DIAGNOSIS_CODE_12 VARCHAR(16777216),
	DIAGNOSIS_CODE_13 VARCHAR(16777216),
	DIAGNOSIS_CODE_14 VARCHAR(16777216),
	DIAGNOSIS_CODE_15 VARCHAR(16777216),
	DIAGNOSIS_CODE_16 VARCHAR(16777216),
	DIAGNOSIS_CODE_17 VARCHAR(16777216),
	DIAGNOSIS_CODE_18 VARCHAR(16777216),
	ICD_INDICATOR VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_DATE_1 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_2 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_DATE_2 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_3 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_DATE_3 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_4 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_DATE_4 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_5 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_DATE_5 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_6 VARCHAR(16777216),
	PRIMARY_PROCEDURE_CODE_DATE_6 VARCHAR(16777216),
	APP_SENDER_CODE VARCHAR(16777216),
	SUBSCRIBER_ID VARCHAR(16777216),
	SERVICE_LINE_NUMBER VARCHAR(16777216),
	REVENUE_CODE VARCHAR(16777216),
	HCPCS_CODE VARCHAR(16777216),
	UNITS_OF_SERVICE VARCHAR,
	SERVICE_CHARGE_AMOUNT VARCHAR,
	CLAIM_PROCESSED_DATE VARCHAR(16777216)
);

USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_LEGACY_INST_CLAIM_EXTRACT"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_INST_CLAIM_EXTRACT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/claim_processed_date=''||:TRAN_MTH;

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_INST_CLAIM_EXTRACT_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''INST_CLAIM_EXTRACT_ERROR'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''INST_CLAIM_EXTRACT'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';




V_STEP_NAME := ''INST_CLAIM_EXTRACT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS (SELECT * FROM ''||V_LZ_TBL||'' WHERE 1 = 2); '';  
 
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
,COALESCE($10, ''''-99999999999999999999'''') 
,$11 
,$12
,$13 
,$14
,$15
,$16
,$17
,$18
,$19
,$20
,$21
,$22
,$23
,$24
,$25
,$26
,$27
,$28
,$29
,$30
,$31
,$32
,$33
,$34
,$35
,$36
,$37
,$38
,$39
,$40
,$41
,$42
,$43
,$44
,$45
,$46
,COALESCE($47, ''''-99999999999999999999'''') 
,COALESCE($48, ''''-99999999999999999999'''') 
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_COMMA_CSV'''', pattern=''''.*claim_processed_date=.*.000.*'''' ;''

;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 

INSERT INTO LZ_EDI_837.INST_CLAIM_EXTRACT_ERROR
SELECT 
  claim_record_type_header , 
  claim_record_type_trailer , 
  claim_number , 
  grp_control_no , 
  trancactset_cntl_no , 
  transactset_create_date , 
  type_of_bill , 
  discharge_status , 
  drg_code , 
  CASE WHEN  medicare_paid_amount = ''-99999999999999999999'' THEN NULL ELSE medicare_paid_amount END , 
  
  primary_diagnosis_code , 
  diagnosis_code_2 , 
  diagnosis_code_3 , 
  diagnosis_code_4 , 
  diagnosis_code_5 , 
  diagnosis_code_6 , 
  diagnosis_code_7 , 
  diagnosis_code_8 , 
  diagnosis_code_9 , 
  diagnosis_code_10 , 
  diagnosis_code_11 , 
  diagnosis_code_12 , 
  diagnosis_code_13 , 
  diagnosis_code_14 , 
  diagnosis_code_15 , 
  diagnosis_code_16 , 
  diagnosis_code_17 , 
  diagnosis_code_18 , 
  icd_indicator , 
  primary_procedure_code , 
  primary_procedure_code_date_1 , 
  primary_procedure_code_2 , 
  primary_procedure_code_date_2 , 
  primary_procedure_code_3 , 
  primary_procedure_code_date_3 , 
  primary_procedure_code_4 , 
  primary_procedure_code_date_4 , 
  primary_procedure_code_5 , 
  primary_procedure_code_date_5 , 
  primary_procedure_code_6 , 
  primary_procedure_code_date_6 , 
  app_sender_code , 
  subscriber_id , 
  service_line_number , 
  revenue_code , 
  hcpcs_code , 
  CASE WHEN  units_of_service = ''-99999999999999999999'' THEN NULL ELSE units_of_service END ,
  CASE WHEN  service_charge_amount = ''-99999999999999999999'' THEN NULL ELSE service_charge_amount END,
  claim_processed_date



FROM IDENTIFIER(:V_TEMP_TBL)
WHERE 
(
   (medicare_paid_amount  NOT regexp ''-?[0-9]+(\.[0-9]+)'' )  OR
   (units_of_service  NOT regexp ''-?[0-9]+(\.[0-9]+)'' )  OR
   (service_charge_amount  NOT regexp ''-?[0-9]+(\.[0-9]+)'' ) 
 
)
;

INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
  claim_record_type_header , 
  claim_record_type_trailer , 
  claim_number , 
  grp_control_no , 
  trancactset_cntl_no , 
  transactset_create_date , 
  type_of_bill , 
  discharge_status , 
  drg_code , 
  medicare_paid_amount , 
  primary_diagnosis_code , 
  diagnosis_code_2 , 
  diagnosis_code_3 , 
  diagnosis_code_4 , 
  diagnosis_code_5 , 
  diagnosis_code_6 , 
  diagnosis_code_7 , 
  diagnosis_code_8 , 
  diagnosis_code_9 , 
  diagnosis_code_10 , 
  diagnosis_code_11 , 
  diagnosis_code_12 , 
  diagnosis_code_13 , 
  diagnosis_code_14 , 
  diagnosis_code_15 , 
  diagnosis_code_16 , 
  diagnosis_code_17 , 
  diagnosis_code_18 , 
  icd_indicator , 
  primary_procedure_code , 
  primary_procedure_code_date_1 , 
  primary_procedure_code_2 , 
  primary_procedure_code_date_2 , 
  primary_procedure_code_3 , 
  primary_procedure_code_date_3 , 
  primary_procedure_code_4 , 
  primary_procedure_code_date_4 , 
  primary_procedure_code_5 , 
  primary_procedure_code_date_5 , 
  primary_procedure_code_6 , 
  primary_procedure_code_date_6 , 
  app_sender_code , 
  subscriber_id , 
  service_line_number , 
  revenue_code , 
  hcpcs_code , 
  units_of_service , 
  service_charge_amount ,
  claim_processed_date ,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  claim_record_type_header , 
  claim_record_type_trailer , 
  claim_number , 
  grp_control_no , 
  trancactset_cntl_no , 
  transactset_create_date , 
  type_of_bill , 
  discharge_status , 
  drg_code , 
  CASE WHEN  medicare_paid_amount = ''-99999999999999999999'' THEN NULL ELSE medicare_paid_amount END , 
  
  primary_diagnosis_code , 
  diagnosis_code_2 , 
  diagnosis_code_3 , 
  diagnosis_code_4 , 
  diagnosis_code_5 , 
  diagnosis_code_6 , 
  diagnosis_code_7 , 
  diagnosis_code_8 , 
  diagnosis_code_9 , 
  diagnosis_code_10 , 
  diagnosis_code_11 , 
  diagnosis_code_12 , 
  diagnosis_code_13 , 
  diagnosis_code_14 , 
  diagnosis_code_15 , 
  diagnosis_code_16 , 
  diagnosis_code_17 , 
  diagnosis_code_18 , 
  icd_indicator , 
  primary_procedure_code , 
  primary_procedure_code_date_1 , 
  primary_procedure_code_2 , 
  primary_procedure_code_date_2 , 
  primary_procedure_code_3 , 
  primary_procedure_code_date_3 , 
  primary_procedure_code_4 , 
  primary_procedure_code_date_4 , 
  primary_procedure_code_5 , 
  primary_procedure_code_date_5 , 
  primary_procedure_code_6 , 
  primary_procedure_code_date_6 , 
  app_sender_code , 
  subscriber_id , 
  service_line_number , 
  revenue_code , 
  hcpcs_code , 
  CASE WHEN  units_of_service = ''-99999999999999999999'' THEN NULL ELSE units_of_service END ,
  CASE WHEN  service_charge_amount = ''-99999999999999999999'' THEN NULL ELSE service_charge_amount END,
  SUBSTR(claim_processed_date, REGEXP_INSTR(claim_processed_date, ''claim_processed_date='')+21, 8) AS claim_processed_date ,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)
WHERE 
(
   (medicare_paid_amount   regexp ''-?[0-9]+(\.[0-9]+)'' )  AND
   (units_of_service   regexp ''-?[0-9]+(\.[0-9]+)'' )  AND
   (service_charge_amount   regexp ''-?[0-9]+(\.[0-9]+)'' ) 
)
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


CREATE OR REPLACE PROCEDURE "SP_LEGACY_DB2IMPORT"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_DB2IMPORT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/ucps_clm_dt=''||SUBSTR(:TRAN_MTH,1,4)||''-''||SUBSTR(:TRAN_MTH,5,2);

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_DB2IMPORT_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''DB2IMPORT_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''DB2IMPORT'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''DB2IMPORT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS (SELECT * EXCLUDE (ISDC_CREATED_DT, ISDC_UPDATED_DT) FROM ''||V_SRC_TBL||'' WHERE 1 = 2); '';  
 
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
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_COMMA_CSV'''', pattern=''''.*ucps_clm_dt=.*.000.*'''' ;''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 


INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
  clm_part_dt , 
  clm_info_key , 
  ucps_clm_num , 
  clm_type , 
  ref_id_qlfr , 
  ref_id_value , 
  member_id , 
  creat_dt ,
  ucps_clm_dt ,
  ISDC_CREATED_DT	,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  clm_part_dt , 
  clm_info_key , 
  ucps_clm_num , 
  clm_type , 
  ref_id_qlfr , 
  ref_id_value , 
  member_id , 
  creat_dt ,
  ucps_clm_dt ,

  
  SUBSTR(ucps_clm_dt, REGEXP_INSTR(ucps_clm_dt, ''ucps_clm_dt='')+12, 10) as ucps_clm_dt,
  
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
