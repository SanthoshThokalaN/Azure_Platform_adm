USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE "SP_LEGACY_PROF_PAYER"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_PAYER'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/transactset_create_date=''||:TRAN_MTH;

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_PAYER_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_PAYER_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_PAYER'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';




V_STEP_NAME := ''PROF_PAYER''; 
   
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
,$10 
,$11 
,$12
,$13 
,$14 
,$15 
,$16 
,$17 
,$18 
,$19 
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_SOH_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''

;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 



INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
  app_sender_code , 
  app_reciever_code , 
  grp_control_no , 
  trancactset_cntl_no , 
  impl_convention_refer , 
  transactset_purpose_code , 
  batch_cntl_no , 
  transactset_create_time , 
  transact_type_code , 
  payer_hl_no , 
  payer_name , 
  identification_cd_qlfy , 
  payer_id , 
  payer_address_1 , 
  payer_address_2 , 
  payer_city , 
  payer_state , 
  payer_postalcode , 
  payer_prior_auth_num ,
  transactset_create_date,
  CLAIM_TRACKING_ID,
  XML_MD5,
  XML_HDR_MD5,
  FILE_SOURCE,
  FILE_NAME,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  PRV.app_sender_code , 
  PRV.app_reciever_code , 
  PRV.grp_control_no , 
  PRV.trancactset_cntl_no , 
  PRV.impl_convention_refer , 
  PRV.transactset_purpose_code , 
  PRV.batch_cntl_no , 
  PRV.transactset_create_time , 
  PRV.transact_type_code , 
  PRV.payer_hl_no , 
  payer_name , 
  identification_cd_qlfy , 
  payer_id , 
  payer_address_1 , 
  payer_address_2 , 
  payer_city , 
  payer_state , 
  payer_postalcode , 
  payer_prior_auth_num ,
  SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
CLM.CLAIM_ID as CLAIM_TRACKING_ID, 
COALESCE(MD5(PRV.GRP_CONTROL_NO||PRV.TRANCACTSET_CNTL_NO||(SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8))||CLM.CLAIM_ID), MD5(''999999999''))   AS XML_MD5,
MD5(PRV.APP_SENDER_CODE||PRV.APP_RECIEVER_CODE||PRV.GRP_CONTROL_NO||PRV.TRANCACTSET_CNTL_NO||PRV.IMPL_CONVENTION_REFER
||PRV.TRANSACTSET_PURPOSE_CODE||PRV.BATCH_CNTL_NO||PRV.TRANSACTSET_CREATE_DATE||PRV.TRANSACTSET_CREATE_TIME||PRV.TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL) PRV LEFT OUTER JOIN 

(

select 
DISTINCT 
GRP_CONTROL_NO,
TRANCACTSET_CNTL_NO,
TRANSACTSET_CREATE_DATE,
CLAIM_ID,
PAYER_HL_NO
FROM LZ_EDI_837.PROF_CLAIM_PART_RAW 
  
) CLM ON 

(CLM.GRP_CONTROL_NO = PRV.GRP_CONTROL_NO AND CLM.TRANCACTSET_CNTL_NO = PRV.TRANCACTSET_CNTL_NO 
AND CLM.transactset_create_date = SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8)
AND CLM.PAYER_HL_NO = PRV.PAYER_HL_NO)




;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :TRAN_MTH, NULL, NULL);




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';



CREATE OR REPLACE PROCEDURE "SP_LEGACY_PROF_PROVIDER"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_PROVIDER'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/transactset_create_date=''||:TRAN_MTH;

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_PROVIDER_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_PROVIDER_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_PROVIDER'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';




V_STEP_NAME := ''PROF_PROVIDER''; 
   
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
,$10 
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
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_SOH_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''

;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 



INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
  app_sender_code , 
  app_reciever_code , 
  grp_control_no , 
  trancactset_cntl_no , 
  impl_convention_refer , 
  transactset_purpose_code , 
  batch_cntl_no , 
  transactset_create_time , 
  transact_type_code , 
  provider_hl_no , 
  provider_tax_code , 
  provider_type , 
  provider_name , 
  provider_name_first , 
  provider_name_middle , 
  name_prefix , 
  name_suffix , 
  provider_id , 
  provider_address_1 , 
  provider_address_2 , 
  provider_city , 
  provider_state , 
  provider_postalcode , 
  refer_id_qlfy , 
  provider_tax_id , 
  provider_contact_name , 
  provider_contact_type , 
  provider_contact_no , 
  payto_ent_type_qlfy , 
  payto_address_1 , 
  payto_address_2 , 
  payto_city , 
  payto_state , 
  payto_zip ,
  transactset_create_date,
  CLAIM_TRACKING_ID,
  XML_MD5,
  XML_HDR_MD5,
  FILE_SOURCE,
  FILE_NAME,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  PRV.app_sender_code , 
  PRV.app_reciever_code , 
  PRV.grp_control_no , 
  PRV.trancactset_cntl_no , 
  PRV.impl_convention_refer , 
  PRV.transactset_purpose_code , 
  PRV.batch_cntl_no , 
  PRV.transactset_create_time , 
  PRV.transact_type_code , 
  PRV.provider_hl_no , 
  provider_tax_code , 
  provider_type , 
  provider_name , 
  provider_name_first , 
  provider_name_middle , 
  name_prefix , 
  name_suffix , 
  provider_id , 
  provider_address_1 , 
  provider_address_2 , 
  provider_city , 
  provider_state , 
  provider_postalcode , 
  refer_id_qlfy , 
  provider_tax_id , 
  provider_contact_name , 
  provider_contact_type , 
  provider_contact_no , 
  payto_ent_type_qlfy , 
  payto_address_1 , 
  payto_address_2 , 
  payto_city , 
  payto_state , 
  payto_zip ,
  SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
CLM.CLAIM_ID as CLAIM_TRACKING_ID, 
COALESCE(MD5(PRV.GRP_CONTROL_NO||PRV.TRANCACTSET_CNTL_NO||(SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8))||CLM.CLAIM_ID), MD5(''999999999''))   AS XML_MD5,
MD5(PRV.APP_SENDER_CODE||PRV.APP_RECIEVER_CODE||PRV.GRP_CONTROL_NO||PRV.TRANCACTSET_CNTL_NO||PRV.IMPL_CONVENTION_REFER
||PRV.TRANSACTSET_PURPOSE_CODE||PRV.BATCH_CNTL_NO||PRV.TRANSACTSET_CREATE_DATE||PRV.TRANSACTSET_CREATE_TIME||PRV.TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL) PRV LEFT OUTER JOIN 

(

select 
DISTINCT 
GRP_CONTROL_NO,
TRANCACTSET_CNTL_NO,
TRANSACTSET_CREATE_DATE,
CLAIM_ID,
provider_hl_no
FROM LZ_EDI_837.PROF_CLAIM_PART_RAW 
  
) CLM ON 

(CLM.GRP_CONTROL_NO = PRV.GRP_CONTROL_NO AND CLM.TRANCACTSET_CNTL_NO = PRV.TRANCACTSET_CNTL_NO 
AND CLM.transactset_create_date = SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8)
AND CLM.provider_hl_no = PRV.provider_hl_no)




;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :TRAN_MTH, NULL, NULL);




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';




CREATE OR REPLACE PROCEDURE "SP_LEGACY_PROF_SUBSCRIBER"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_SUBSCRIBER'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/transactset_create_date=''||:TRAN_MTH;

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_SUBSCRIBER_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_SUBSCRIBER_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_SUBSCRIBER'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';




V_STEP_NAME := ''PROF_SUBSCRIBER''; 
   
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
,$10 
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
,$47 
,$48 
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_SOH_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''

;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 

INSERT INTO LZ_EDI_837.PROF_SUBSCRIBER_ERROR
SELECT * FROM IDENTIFIER(:V_TEMP_TBL)
WHERE (OTHER_SUBSCRIBER_TYPE NOT regexp ''[0-9]+'' AND OTHER_SUBSCRIBER_TYPE IS NOT NULL)
 ;

INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
  app_sender_code , 
  app_reciever_code , 
  grp_control_no , 
  trancactset_cntl_no , 
  impl_convention_refer , 
  transactset_purpose_code , 
  batch_cntl_no , 
  transactset_create_time , 
  transact_type_code , 
  subscriber_hl_no , 
  payer_responsibility_code , 
  subscriber_relationship_code , 
  subscriber_policy_number , 
  subscriber_plan_name , 
  subscriber_insurance_type , 
  subscriber_cob_code , 
  yes_no_response_code , 
  subscriber_employement_status , 
  claim_type , 
  subscriber_type , 
  subscriber_name_last , 
  subscriber_name_first , 
  subscriber_name_middle , 
  name_prefix , 
  name_suffix , 
  subscriber_id_qualifier , 
  subscriber_id , 
  subscriber_address1 , 
  subscriber_address2 , 
  subscriber_city , 
  subscriber_state , 
  subscriber_postalcode , 
  subscriber_dateofbirth , 
  subscriber_gendercode , 
  subscriber_suplemental_id_qlfr , 
  subscriber_sumplemental_id , 
  other_subscriber_type , 
  other_subscriber_name_last , 
  other_subscriber_name_first , 
  other_subscriber_name_middle , 
  other_subscriber_suffix , 
  other_subscriber_id_qlfr , 
  other_subscriber_id , 
  other_subscriber_address1 , 
  other_subscriber_address2 , 
  other_subscriber_city , 
  other_subscriber_state , 
  other_subscriber_zip ,
  transactset_create_date,
  CLAIM_TRACKING_ID,
  XML_MD5,
  XML_HDR_MD5,
  FILE_SOURCE,
  FILE_NAME,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  PRV.app_sender_code , 
  PRV.app_reciever_code , 
  PRV.grp_control_no , 
  PRV.trancactset_cntl_no , 
  PRV.impl_convention_refer , 
  PRV.transactset_purpose_code , 
  PRV.batch_cntl_no , 
  PRV.transactset_create_time , 
  PRV.transact_type_code , 
  PRV.subscriber_hl_no , 
  payer_responsibility_code , 
  subscriber_relationship_code , 
  subscriber_policy_number , 
  subscriber_plan_name , 
  subscriber_insurance_type , 
  subscriber_cob_code , 
  yes_no_response_code , 
  subscriber_employement_status , 
  claim_type , 
  subscriber_type , 
  subscriber_name_last , 
  subscriber_name_first , 
  subscriber_name_middle , 
  name_prefix , 
  name_suffix , 
  subscriber_id_qualifier , 
  subscriber_id , 
  subscriber_address1 , 
  subscriber_address2 , 
  subscriber_city , 
  subscriber_state , 
  subscriber_postalcode , 
  subscriber_dateofbirth , 
  subscriber_gendercode , 
  subscriber_suplemental_id_qlfr , 
  subscriber_sumplemental_id , 
  other_subscriber_type , 
  other_subscriber_name_last , 
  other_subscriber_name_first , 
  other_subscriber_name_middle , 
  other_subscriber_suffix , 
  other_subscriber_id_qlfr , 
  other_subscriber_id , 
  other_subscriber_address1 , 
  other_subscriber_address2 , 
  other_subscriber_city , 
  other_subscriber_state , 
  other_subscriber_zip ,
  SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
CLM.CLAIM_ID as CLAIM_TRACKING_ID, 
COALESCE(MD5(PRV.GRP_CONTROL_NO||PRV.TRANCACTSET_CNTL_NO||(SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8))||CLM.CLAIM_ID), MD5(''999999999''))   AS XML_MD5,
MD5(PRV.APP_SENDER_CODE||PRV.APP_RECIEVER_CODE||PRV.GRP_CONTROL_NO||PRV.TRANCACTSET_CNTL_NO||PRV.IMPL_CONVENTION_REFER
||PRV.TRANSACTSET_PURPOSE_CODE||PRV.BATCH_CNTL_NO||PRV.TRANSACTSET_CREATE_DATE||PRV.TRANSACTSET_CREATE_TIME||PRV.TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL) PRV LEFT OUTER JOIN 

(

select 
DISTINCT 
GRP_CONTROL_NO,
TRANCACTSET_CNTL_NO,
TRANSACTSET_CREATE_DATE,
CLAIM_ID,
SUBSCRIBER_HL_NO
FROM LZ_EDI_837.PROF_CLAIM_PART_RAW 
  
) CLM ON 

(CLM.GRP_CONTROL_NO = PRV.GRP_CONTROL_NO AND CLM.TRANCACTSET_CNTL_NO = PRV.TRANCACTSET_CNTL_NO 
AND CLM.transactset_create_date = SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8)
AND CLM.SUBSCRIBER_HL_NO = PRV.SUBSCRIBER_HL_NO)

WHERE (PRV.OTHER_SUBSCRIBER_TYPE regexp ''[0-9]'' OR PRV.OTHER_SUBSCRIBER_TYPE IS NULL)


;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :TRAN_MTH, NULL, NULL);




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';


CREATE OR REPLACE PROCEDURE "SP_LEGACY_PROF_PATIENT"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_PATIENT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/transactset_create_date=''||:TRAN_MTH;

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_PATIENT_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_PATIENT_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_PATIENT'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';




V_STEP_NAME := ''PROF_PATIENT''; 
   
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
,$10 
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
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_SOH_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''

;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 



INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
  app_sender_code , 
  app_reciever_code , 
  grp_control_no , 
  trancactset_cntl_no , 
  impl_convention_refer , 
  transactset_purpose_code , 
  batch_cntl_no , 
  transactset_create_time , 
  transact_type_code , 
  provider_hl_no , 
  subscriber_hl_no , 
  payer_hl_no , 
  pat_death_dt , 
  pat_weight_measure_unit , 
  pat_weight , 
  pat_pregnancy_indicator , 
  pat_insurer_relationship_cd , 
  pat_dep_death_dt , 
  pat_dep_weight_measure_unit , 
  pat_dep_weight , 
  pat_dep_pregnancy_indicator , 
  pat_type_qlfr , 
  pat_name_last , 
  pat_name_first , 
  pat_name_middle , 
  pat_name_suffix , 
  pat_address_1 , 
  pat_address_2 , 
  pat_city , 
  pat_state , 
  pat_zip , 
  pat_dateofbirth , 
  pat_gendercode ,
  transactset_create_date,
  CLAIM_TRACKING_ID,
  XML_MD5,
  XML_HDR_MD5,
  FILE_SOURCE,
  FILE_NAME,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  PRV.app_sender_code , 
  PRV.app_reciever_code , 
  PRV.grp_control_no , 
  PRV.trancactset_cntl_no , 
  PRV.impl_convention_refer , 
  PRV.transactset_purpose_code , 
  PRV.batch_cntl_no , 
  PRV.transactset_create_time , 
  PRV.transact_type_code , 
  PRV.provider_hl_no , 
  PRV.subscriber_hl_no , 
  PRV.payer_hl_no , 
  pat_death_dt , 
  pat_weight_measure_unit , 
  pat_weight , 
  pat_pregnancy_indicator , 
  pat_insurer_relationship_cd , 
  pat_dep_death_dt , 
  pat_dep_weight_measure_unit , 
  pat_dep_weight , 
  pat_dep_pregnancy_indicator , 
  pat_type_qlfr , 
  pat_name_last , 
  pat_name_first , 
  pat_name_middle , 
  pat_name_suffix , 
  pat_address_1 , 
  pat_address_2 , 
  pat_city , 
  pat_state , 
  pat_zip , 
  pat_dateofbirth , 
  pat_gendercode ,
  SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
CLM.CLAIM_ID as CLAIM_TRACKING_ID, 
COALESCE(MD5(PRV.GRP_CONTROL_NO||PRV.TRANCACTSET_CNTL_NO||(SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8))||CLM.CLAIM_ID), MD5(''999999999''))   AS XML_MD5,
MD5(PRV.APP_SENDER_CODE||PRV.APP_RECIEVER_CODE||PRV.GRP_CONTROL_NO||PRV.TRANCACTSET_CNTL_NO||PRV.IMPL_CONVENTION_REFER
||PRV.TRANSACTSET_PURPOSE_CODE||PRV.BATCH_CNTL_NO||PRV.TRANSACTSET_CREATE_DATE||PRV.TRANSACTSET_CREATE_TIME||PRV.TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL) PRV LEFT OUTER JOIN 

(

select 
DISTINCT 
GRP_CONTROL_NO,
TRANCACTSET_CNTL_NO,
TRANSACTSET_CREATE_DATE,
CLAIM_ID,
SUBSCRIBER_HL_NO
FROM LZ_EDI_837.PROF_CLAIM_PART_RAW 
  
) CLM ON 

(CLM.GRP_CONTROL_NO = PRV.GRP_CONTROL_NO AND CLM.TRANCACTSET_CNTL_NO = PRV.TRANCACTSET_CNTL_NO 
AND CLM.transactset_create_date = SUBSTR(PRV.transactset_create_date, REGEXP_INSTR(PRV.transactset_create_date, ''transactset_create_date='')+24, 8)
AND CLM.SUBSCRIBER_HL_NO = PRV.SUBSCRIBER_HL_NO)




;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :TRAN_MTH, NULL, NULL);




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
