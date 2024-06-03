USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_LEGACY_PROF_CLAIM_PART"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_CLAIM_PART'';

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

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_CLAIM_PART_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_CLAIM_PART_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_CLAIM_PART'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';




V_STEP_NAME := ''PROF_CLAIM_PART''; 
   
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
,COALESCE($21, ''''-99999999999999999999'''') 
,COALESCE($22, ''''-99999999999999999999'''') 
,$23 
,COALESCE($24, ''''99999999999999999999'''')
,$25 
,COALESCE($26, ''''-99999999999999999999'''') 
,$27 
,COALESCE($28, ''''-99999999999999999999'''') 
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
,split($46,'''''''') 
,$47 
,split($48,'''''''') 
,$49 
,$50 
,$51 
,$52 
,$53 
,$54
,$55 
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_SOH_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''

;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 


TRUNCATE TABLE IDENTIFIER(:V_LZ_TBL);

INSERT INTO IDENTIFIER(:V_LZ_TBL)
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
  claim_id , 
  transactset_create_date
)
 
SELECT DISTINCT   
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
  claim_id , 
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date

FROM IDENTIFIER(:V_TEMP_TBL)
;


INSERT INTO LZ_EDI_837.PROF_CLAIM_PART_ERROR

SELECT 
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
  claim_id , 
  total_claim_charge_amt , 
  healthcareservice_location , 
  provider_accept_assign_code , 
  provider_benefit_auth_code , 
  provider_patinfo_release_auth_code , 
  delay_reason_code , 
  health_care_code_info , 
  CASE WHEN sv_reimbursement_rate = ''-99999999999999999999'' THEN NULL ELSE sv_reimbursement_rate END,
  CASE WHEN sv_hcpcs_payable_amt = ''-99999999999999999999'' THEN NULL ELSE sv_hcpcs_payable_amt END,
  sv_clm_payment_remark_code, 
  CASE WHEN  sl_seq_num = ''99999999999999999999'' THEN NULL ELSE sl_seq_num END ,
  product_service_id_qlfr ,
  CASE WHEN line_item_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_charge_amt END,
  measurement_unit, 
  CASE WHEN  service_unit_count = ''-99999999999999999999'' THEN NULL ELSE service_unit_count END , 
  date_time_qlfy , 
  date_time_frmt_qlfy , 
  service_date , 
  cas_adj_group_code , 
  cas_adj_reason_code , 
  cas_adj_amt , 
  vendor_cd , 
  provider_sign_indicator , 
  pat_signature_cd , 
  related_cause_code_info , 
  special_program_indicator , 
  patient_amt_paid , 
  service_auth_exception_code , 
  clinical_lab_amendment_num , 
  network_trace_number , 
  medical_record_number , 
  demonstration_project_id , 
  health_care_additional_code_info , 
  anesthesia_procedure_code , 
  hc_condition_codes , 
  payer_clm_ctrl_num , 
  non_covered_charge_amt , 
  diagnosis_code_pointers , 
  emergency_indicator , 
  family_planning_indicator , 
  epsdt_indicator , 
  clm_billing_note_text ,
  transactset_create_date
FROM IDENTIFIER(:V_TEMP_TBL)
WHERE 
(
   (SV_REIMBURSEMENT_RATE   NOT regexp ''-?[0-9]+(\.[0-9]+)'' )  OR
   (SV_HCPCS_PAYABLE_AMT   NOT regexp ''-?[0-9]+(\.[0-9]+)'' )  OR
   (SL_SEQ_NUM   NOT regexp ''[0-9]+'')  OR
   (LINE_ITEM_CHARGE_AMT   NOT regexp ''-?[0-9]+(\.[0-9]+)'' )  OR
   (SERVICE_UNIT_COUNT   NOT regexp ''-?[0-9]+(\.[0-9]+)'' )  
)
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
  provider_hl_no , 
  subscriber_hl_no , 
  payer_hl_no , 
  claim_id , 
  total_claim_charge_amt , 
  healthcareservice_location , 
  provider_accept_assign_code , 
  provider_benefit_auth_code , 
  provider_patinfo_release_auth_code , 
  delay_reason_code , 
  health_care_code_info , 
  sv_reimbursement_rate , 
  sv_hcpcs_payable_amt , 
  sv_clm_payment_remark_code , 
  sl_seq_num , 
  product_service_id_qlfr , 
  line_item_charge_amt , 
  measurement_unit , 
  service_unit_count , 
  date_time_qlfy , 
  date_time_frmt_qlfy , 
  service_date , 
  cas_adj_group_code , 
  cas_adj_reason_code , 
  cas_adj_amt , 
  vendor_cd , 
  provider_sign_indicator , 
  pat_signature_cd , 
  related_cause_code_info , 
  special_program_indicator , 
  patient_amt_paid , 
  service_auth_exception_code , 
  clinical_lab_amendment_num , 
  network_trace_number , 
  medical_record_number , 
  demonstration_project_id , 
  health_care_additional_code_info , 
  anesthesia_procedure_code , 
  hc_condition_codes , 
  payer_clm_ctrl_num , 
  non_covered_charge_amt , 
  diagnosis_code_pointers , 
  emergency_indicator , 
  family_planning_indicator , 
  epsdt_indicator , 
  clm_billing_note_text ,
  transactset_create_date ,
  CLAIM_TRACKING_ID,
  XML_MD5,
  XML_HDR_MD5,
  FILE_SOURCE,
  FILE_NAME,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
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
  claim_id , 
  total_claim_charge_amt , 
  healthcareservice_location , 
  provider_accept_assign_code , 
  provider_benefit_auth_code , 
  provider_patinfo_release_auth_code , 
  delay_reason_code , 
  health_care_code_info , 
  CASE WHEN sv_reimbursement_rate = ''-99999999999999999999'' THEN NULL ELSE sv_reimbursement_rate END,
  CASE WHEN sv_hcpcs_payable_amt = ''-99999999999999999999'' THEN NULL ELSE sv_hcpcs_payable_amt END,
  sv_clm_payment_remark_code , 
  CASE WHEN  sl_seq_num = ''99999999999999999999'' THEN NULL ELSE sl_seq_num END , 
  product_service_id_qlfr , 
  CASE WHEN line_item_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_charge_amt END,
  measurement_unit , 
  CASE WHEN  service_unit_count = ''-99999999999999999999'' THEN NULL ELSE service_unit_count END , 
  date_time_qlfy , 
  date_time_frmt_qlfy , 
  service_date , 
  cas_adj_group_code , 
  cas_adj_reason_code , 
  cas_adj_amt , 
  vendor_cd , 
  provider_sign_indicator , 
  pat_signature_cd , 
  related_cause_code_info , 
  special_program_indicator , 
  patient_amt_paid , 
  service_auth_exception_code , 
  clinical_lab_amendment_num , 
  network_trace_number , 
  medical_record_number , 
  demonstration_project_id , 
  health_care_additional_code_info , 
  anesthesia_procedure_code , 
  hc_condition_codes , 
  payer_clm_ctrl_num , 
  non_covered_charge_amt , 
  diagnosis_code_pointers , 
  emergency_indicator , 
  family_planning_indicator , 
  epsdt_indicator , 
  clm_billing_note_text ,
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
NULL as CLAIM_TRACKING_ID, 
NULL   AS XML_MD5,
MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)
WHERE 
(
   (SV_REIMBURSEMENT_RATE   regexp ''-?[0-9]+(\.[0-9]+)'' )  AND
   (SV_HCPCS_PAYABLE_AMT   regexp ''-?[0-9]+(\.[0-9]+)'' )  AND
   (SL_SEQ_NUM   regexp ''[0-9]+'')  AND
   (LINE_ITEM_CHARGE_AMT   regexp ''-?[0-9]+(\.[0-9]+)'' )  AND
   (SERVICE_UNIT_COUNT   regexp ''-?[0-9]+(\.[0-9]+)'' )  

)
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

USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_LEGACY_PROF_PROVIDER_ALL("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_PROVIDER_ALL'';

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

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_PROVIDER_ALL_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_PROVIDER_ALL_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_PROVIDER_ALL'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''PROF_PROVIDER_ALL''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS (SELECT * FROM ''||V_LZ_TBL||'' WHERE 1 = 2); '';  
 
V_QUERY2 := '' 

COPY INTO ''||:V_TEMP_TBL||'' FROM  (
select 
$1 ,
        $2 ,
        $3 ,
        $4 ,
        $5 ,
        $6 ,
        $7 ,
        $8 ,
        $9 ,
        $10 ,
        $11 ,
        $12 ,
        $13 ,
        STRTOK_TO_ARRAY(replace($14,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($15,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($16,'''''''',''''~''''),'''''''') ,
        $17 ,
        STRTOK_TO_ARRAY(replace($18,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($19,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($20,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($21,'''''''',''''~''''),'''''''') ,
metadata$filename
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
  claim_id , 
  clm_rendering_prv_map , 
  clm_supervising_prv_map , 
  clm_referring_prv_map , 
  sl_seq_num , 
  sv_rendering_prv_map , 
  sv_supervising_prv_map , 
  sv_referring_prv_map , 
  sv_ordering_prv_map ,
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
  claim_id , 
CASE WHEN clm_rendering_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_rendering_prv_map[0]::VARCHAR  ,replace(clm_rendering_prv_map[1]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[2]::VARCHAR  ,replace(clm_rendering_prv_map[3]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[4]::VARCHAR  ,replace(clm_rendering_prv_map[5]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[6]::VARCHAR  ,replace(clm_rendering_prv_map[7]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[8]::VARCHAR  ,replace(clm_rendering_prv_map[9]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[10]::VARCHAR ,replace(clm_rendering_prv_map[11]::VARCHAR,''~'') ,
                         clm_rendering_prv_map[12]::VARCHAR ,replace(clm_rendering_prv_map[13]::VARCHAR,''~'') ,
                         clm_rendering_prv_map[14]::VARCHAR ,replace(clm_rendering_prv_map[15]::VARCHAR,''~'') ,
                         clm_rendering_prv_map[16]::VARCHAR ,NULLIF(replace(clm_rendering_prv_map[17]::VARCHAR,''~''),''\\\\N'') ,
                         clm_rendering_prv_map[18]::VARCHAR ,NULLIF(replace(clm_rendering_prv_map[19]::VARCHAR,''~''),''\\\\N'') ,
                         clm_rendering_prv_map[20]::VARCHAR ,replace(clm_rendering_prv_map[21]::VARCHAR,''~'')) END clm_rendering_prv_map, 
  CASE WHEN clm_supervising_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_supervising_prv_map[0]::VARCHAR  ,replace(clm_supervising_prv_map[1]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[2]::VARCHAR  ,replace(clm_supervising_prv_map[3]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[4]::VARCHAR  ,replace(clm_supervising_prv_map[5]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[6]::VARCHAR  ,replace(clm_supervising_prv_map[7]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[8]::VARCHAR  ,replace(clm_supervising_prv_map[9]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[10]::VARCHAR ,replace(clm_supervising_prv_map[11]::VARCHAR,''~'') ,
                         clm_supervising_prv_map[12]::VARCHAR ,replace(clm_supervising_prv_map[13]::VARCHAR,''~'') ,
                         clm_supervising_prv_map[14]::VARCHAR ,replace(clm_supervising_prv_map[15]::VARCHAR,''~'') ) END clm_supervising_prv_map,  
  CASE WHEN clm_referring_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_referring_prv_map[0]::VARCHAR  ,replace(clm_referring_prv_map[1]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[2]::VARCHAR  ,replace(clm_referring_prv_map[3]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[4]::VARCHAR  ,replace(clm_referring_prv_map[5]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[6]::VARCHAR  ,replace(clm_referring_prv_map[7]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[8]::VARCHAR  ,replace(clm_referring_prv_map[9]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[10]::VARCHAR ,replace(clm_referring_prv_map[11]::VARCHAR,''~'') ,
                         clm_referring_prv_map[12]::VARCHAR ,replace(clm_referring_prv_map[13]::VARCHAR,''~'') ,
                         clm_referring_prv_map[14]::VARCHAR ,replace(clm_referring_prv_map[15]::VARCHAR,''~'') ) END clm_referring_prv_map,   
  sl_seq_num , 
  CASE WHEN sv_rendering_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(sv_rendering_prv_map[0]::VARCHAR  ,replace(sv_rendering_prv_map[1]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[2]::VARCHAR  ,replace(sv_rendering_prv_map[3]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[4]::VARCHAR  ,replace(sv_rendering_prv_map[5]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[6]::VARCHAR  ,replace(sv_rendering_prv_map[7]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[8]::VARCHAR  ,replace(sv_rendering_prv_map[9]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[10]::VARCHAR ,replace(sv_rendering_prv_map[11]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[12]::VARCHAR ,replace(sv_rendering_prv_map[13]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[14]::VARCHAR ,replace(sv_rendering_prv_map[15]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[16]::VARCHAR ,replace(sv_rendering_prv_map[17]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[18]::VARCHAR ,replace(sv_rendering_prv_map[19]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[20]::VARCHAR ,replace(sv_rendering_prv_map[21]::VARCHAR,''~'') ) END sv_rendering_prv_map, 
  CASE WHEN sv_supervising_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE                  
        OBJECT_CONSTRUCT(sv_supervising_prv_map[0]::VARCHAR  ,replace(sv_supervising_prv_map[1]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[2]::VARCHAR  ,replace(sv_supervising_prv_map[3]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[4]::VARCHAR  ,replace(sv_supervising_prv_map[5]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[6]::VARCHAR  ,replace(sv_supervising_prv_map[7]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[8]::VARCHAR  ,replace(sv_supervising_prv_map[9]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[10]::VARCHAR ,replace(sv_supervising_prv_map[11]::VARCHAR,''~'') ,
                         sv_supervising_prv_map[12]::VARCHAR ,replace(sv_supervising_prv_map[13]::VARCHAR,''~'') ,
                         sv_supervising_prv_map[14]::VARCHAR ,replace(sv_supervising_prv_map[15]::VARCHAR,''~'') ,
                         sv_supervising_prv_map[16]::VARCHAR ,replace(sv_supervising_prv_map[17]::VARCHAR,''~'') ) END sv_supervising_prv_map, 
  CASE WHEN sv_referring_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE                  
        OBJECT_CONSTRUCT(sv_referring_prv_map[0]::VARCHAR  ,replace(sv_referring_prv_map[1]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[2]::VARCHAR  ,replace(sv_referring_prv_map[3]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[4]::VARCHAR  ,replace(sv_referring_prv_map[5]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[6]::VARCHAR  ,replace(sv_referring_prv_map[7]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[8]::VARCHAR  ,replace(sv_referring_prv_map[9]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[10]::VARCHAR ,replace(sv_referring_prv_map[11]::VARCHAR,''~'') ,
                         sv_referring_prv_map[12]::VARCHAR ,replace(sv_referring_prv_map[13]::VARCHAR,''~'') ,
                         sv_referring_prv_map[14]::VARCHAR ,replace(sv_referring_prv_map[15]::VARCHAR,''~'') ,
                         sv_referring_prv_map[16]::VARCHAR ,replace(sv_referring_prv_map[17]::VARCHAR,''~'') ) END sv_referring_prv_map, 
  CASE WHEN sv_ordering_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE                  
        OBJECT_CONSTRUCT(sv_ordering_prv_map[0]::VARCHAR  ,replace(sv_ordering_prv_map[1]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[2]::VARCHAR  ,replace(sv_ordering_prv_map[3]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[4]::VARCHAR  ,replace(sv_ordering_prv_map[5]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[6]::VARCHAR  ,replace(sv_ordering_prv_map[7]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[8]::VARCHAR  ,replace(sv_ordering_prv_map[9]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[10]::VARCHAR ,replace(sv_ordering_prv_map[11]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[12]::VARCHAR ,replace(sv_ordering_prv_map[13]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[14]::VARCHAR ,replace(sv_ordering_prv_map[15]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[16]::VARCHAR ,replace(sv_ordering_prv_map[17]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[18]::VARCHAR ,replace(sv_ordering_prv_map[19]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[20]::VARCHAR ,replace(sv_ordering_prv_map[21]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[22]::VARCHAR ,replace(sv_ordering_prv_map[23]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[24]::VARCHAR ,replace(sv_ordering_prv_map[25]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[26]::VARCHAR ,replace(sv_ordering_prv_map[27]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[28]::VARCHAR ,replace(sv_ordering_prv_map[29]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[30]::VARCHAR ,replace(sv_ordering_prv_map[31]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[32]::VARCHAR ,replace(sv_ordering_prv_map[33]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[34]::VARCHAR ,replace(sv_ordering_prv_map[35]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[36]::VARCHAR ,replace(sv_ordering_prv_map[37]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[38]::VARCHAR ,replace(sv_ordering_prv_map[39]::VARCHAR,''~'') ) END sv_ordering_prv_map,
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
NULL as CLAIM_TRACKING_ID, 


MD5(GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||(SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8))||CLAIM_ID)   AS XML_MD5,


MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
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







USE SCHEMA SRC_EDI_837;


CREATE OR REPLACE PROCEDURE SP_LEGACY_PROF_AMBLNC_INFO("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_AMBLNC_INFO'';

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

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_AMBLNC_INFO_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_AMBLNC_INFO_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_AMBLNC_INFO'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''PROF_AMBLNC_INFO''; 
   
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
,STRTOK_TO_ARRAY(replace(replace($32,'''''''',''''~~''''),'''''''',''''~''''),'''''''')
,$33 
,$34 
,$35 
,$36 
,$37 
,$38 
,$39 
,$40 
,STRTOK_TO_ARRAY(replace(replace($41,'''''''',''''~~''''),'''''''',''''~''''),'''''''')
,$42 
,$43 
,$44 
,$45 
,$46 
,$47 
,$48 
,$49 
,$50 
,$51 
,$52 
,$53 
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
  claim_id , 
  clm_amblnc_weight_measure_unit , 
  clm_amblnc_pat_weight , 
  clm_amblnc_transport_reason_cd , 
  clm_amblnc_distance_measure_unit , 
  clm_amblnc_transport_distance , 
  clm_amblnc_roundtrip_desc , 
  clm_amblnc_stretcher_desc , 
  clm_amblnc_pickup_addr_1 , 
  clm_amblnc_pickup_addr_2 , 
  clm_amblnc_pickup_city , 
  clm_amblnc_pickup_state , 
  clm_amblnc_pickup_zip , 
  clm_amblnc_dropoff_location , 
  clm_amblnc_dropoff_addr_1 , 
  clm_amblnc_dropoff_addr_2 , 
  clm_amblnc_dropoff_city , 
  clm_amblnc_dropoff_state , 
  clm_amblnc_dropoff_zip , 
  clm_amblnc_certification , 
  sv_lx_number , 
  sv_amblnc_weight_measure_unit , 
  sv_amblnc_pat_weight , 
  sv_amblnc_transport_reason_cd , 
  sv_amblnc_distance_measure_unit , 
  sv_amblnc_transport_distance , 
  sv_amblnc_roundtrip_desc , 
  sv_amblnc_stretcher_desc , 
  sv_amblnc_certification ,  
  sv_amblnc_pat_count , 
  sv_amblnc_pickup_addr_1 , 
  sv_amblnc_pickup_addr_2 , 
  sv_amblnc_pickup_city , 
  sv_amblnc_pickup_state , 
  sv_amblnc_pickup_zip , 
  sv_amblnc_dropoff_location , 
  sv_amblnc_dropoff_addr_1 , 
  sv_amblnc_dropoff_addr_2 , 
  sv_amblnc_dropoff_city , 
  sv_amblnc_dropoff_state , 
  sv_amblnc_dropoff_zip ,
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
  claim_id , 
  clm_amblnc_weight_measure_unit , 
  clm_amblnc_pat_weight , 
  clm_amblnc_transport_reason_cd , 
  clm_amblnc_distance_measure_unit , 
  clm_amblnc_transport_distance , 
  clm_amblnc_roundtrip_desc , 
  clm_amblnc_stretcher_desc , 
  clm_amblnc_pickup_addr_1 , 
  clm_amblnc_pickup_addr_2 , 
  clm_amblnc_pickup_city , 
  clm_amblnc_pickup_state , 
  clm_amblnc_pickup_zip , 
  clm_amblnc_dropoff_location , 
  clm_amblnc_dropoff_addr_1 , 
  clm_amblnc_dropoff_addr_2 , 
  clm_amblnc_dropoff_city , 
  clm_amblnc_dropoff_state , 
  clm_amblnc_dropoff_zip , 
  ARRAY_CONSTRUCT( 
                         OBJECT_CONSTRUCT( ''clm_amblnc_cert_condition_indicator'', replace(clm_amblnc_certification[0],''~''),
                                           ''clm_amblnc_condition_codes'', array_construct(replace(clm_amblnc_certification[1],''~''),replace(clm_amblnc_certification[2],''~''), 
                                                                                         replace(clm_amblnc_certification[3],''~''),replace(clm_amblnc_certification[4],''~''), 
                                                                                         replace(clm_amblnc_certification[5],''~''))), 
                         OBJECT_CONSTRUCT(''clm_amblnc_cert_condition_indicator'', replace(clm_amblnc_certification[6],''~''),
                                          ''clm_amblnc_condition_codes'', array_construct(replace(clm_amblnc_certification[7],''~''),replace(clm_amblnc_certification[8],''~''), 
                                                                                        replace(clm_amblnc_certification[9],''~''),replace(clm_amblnc_certification[10],''~''), 
                                                                                        replace(clm_amblnc_certification[11],''~''))), 
                         OBJECT_CONSTRUCT(''clm_amblnc_cert_condition_indicator'', replace(clm_amblnc_certification[12],''~''),
                                          ''clm_amblnc_condition_codes'', array_construct(replace(clm_amblnc_certification[13],''~''), replace(clm_amblnc_certification[14],''~''), 
                                                                                        replace(clm_amblnc_certification[15],''~''), replace(clm_amblnc_certification[16],''~''), 
                                                                                        replace(clm_amblnc_certification[17],''~'')))) clm_amblnc_certification , 
  sv_lx_number , 
  sv_amblnc_weight_measure_unit , 
  sv_amblnc_pat_weight , 
  sv_amblnc_transport_reason_cd , 
  sv_amblnc_distance_measure_unit , 
  sv_amblnc_transport_distance , 
  sv_amblnc_roundtrip_desc , 
  sv_amblnc_stretcher_desc , 
          ARRAY_CONSTRUCT( 
                         OBJECT_CONSTRUCT( ''sv_amblnc_cert_condition_indicator'', replace(sv_amblnc_certification[0],''~''),
                                           ''sv_amblnc_condition_codes'', array_construct(replace(sv_amblnc_certification[1],''~''),replace(sv_amblnc_certification[2],''~''), 
                                                                                         replace(sv_amblnc_certification[3],''~''),replace(sv_amblnc_certification[4],''~''), 
                                                                                         replace(sv_amblnc_certification[5],''~''))), 
                         OBJECT_CONSTRUCT(''sv_amblnc_cert_condition_indicator'', replace(sv_amblnc_certification[6],''~''),
                                          ''sv_amblnc_condition_codes'', array_construct(replace(sv_amblnc_certification[7],''~''),replace(sv_amblnc_certification[8],''~''), 
                                                                                        replace(sv_amblnc_certification[9],''~''),replace(sv_amblnc_certification[10],''~''), 
                                                                                        replace(sv_amblnc_certification[11],''~''))), 
                         OBJECT_CONSTRUCT(''sv_amblnc_cert_condition_indicator'', replace(sv_amblnc_certification[12],''~''),
                                          ''sv_amblnc_condition_codes'', array_construct(replace(sv_amblnc_certification[13],''~''), replace(sv_amblnc_certification[14],''~''), 
                                                                                        replace(sv_amblnc_certification[15],''~''), replace(sv_amblnc_certification[16],''~''), 
                                                                                        replace(sv_amblnc_certification[17],''~'')))) sv_amblnc_certification ,  
  sv_amblnc_pat_count , 
  sv_amblnc_pickup_addr_1 , 
  sv_amblnc_pickup_addr_2 , 
  sv_amblnc_pickup_city , 
  sv_amblnc_pickup_state , 
  sv_amblnc_pickup_zip , 
  sv_amblnc_dropoff_location , 
  sv_amblnc_dropoff_addr_1 , 
  sv_amblnc_dropoff_addr_2 , 
  sv_amblnc_dropoff_city , 
  sv_amblnc_dropoff_state , 
  sv_amblnc_dropoff_zip ,
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) as transactset_create_date,
NULL as CLAIM_TRACKING_ID, 
MD5(GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||(SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8))||CLAIM_ID)   AS XML_MD5,
MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
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


USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_LEGACY_PROF_SERVICE("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_SERVICE'';

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

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_SERVICE_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_SERVICE_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_SERVICE'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''PROF_SERVICE''; 
   
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
,STRTOK_TO_ARRAY(REPLACE($22,'''''''',''''~~''''),'''''''') 
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
  claim_id , 
  sl_seq_num , 
  dmec_mni_cert_transmission_cd , 
  dmec_type_cd , 
  dmec_duration_measure_unit , 
  dmec_duration , 
  dmec_mni_condition_indicator , 
  dmec_mni_condition_cd_1 , 
  dmec_mni_condition_cd_2 , 
  test_measure_results, 
  clincal_lab_imprvment_amndment_num , 
  referring_clia_number , 
  medical_procedure_id , 
  medical_necessity_measure_unit , 
  medical_necessity_length , 
  dme_rental_price , 
  dme_purchase_price , 
  dme_frequency_cd , 
  sv_facility_type_qlfr , 
  sv_facility_name , 
  sv_facility_primary_id , 
  sv_facility_addr_1 , 
  sv_facility_addr_2 , 
  sv_facility_city , 
  sv_facility_state , 
  sv_facility_zip , 
  sv_facility_ref_id_qlfr_1 , 
  sv_facility_secondary_id_1 , 
  sv_facility_secondary_id_2 , 
  drug_product_id_qlfr , 
  drug_product_id , 
  drug_unit_count , 
  drug_measure_unit ,
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
  claim_id , 
  sl_seq_num , 
  dmec_mni_cert_transmission_cd , 
  dmec_type_cd , 
  dmec_duration_measure_unit , 
  dmec_duration , 
  dmec_mni_condition_indicator , 
  dmec_mni_condition_cd_1 , 
  dmec_mni_condition_cd_2 , 
  array_construct(OBJECT_CONSTRUCT(''test_measure_ref_id''  ,replace(test_measure_results[0],''~''),
                                 ''test_measure_type''    ,replace(test_measure_results[1],''~''),
                                 ''test_measure_results'' ,replace(test_measure_results[2],''~'')),
                OBJECT_CONSTRUCT(''test_measure_ref_id''  ,replace(test_measure_results[3],''~''),
                                 ''test_measure_type''    ,replace(test_measure_results[4],''~''),
                                 ''test_measure_results'' ,replace(test_measure_results[5],''~''))) test_measure_results, 
  clincal_lab_imprvment_amndment_num , 
  referring_clia_number , 
  medical_procedure_id , 
  medical_necessity_measure_unit , 
  medical_necessity_length , 
  dme_rental_price , 
  dme_purchase_price , 
  dme_frequency_cd , 
  sv_facility_type_qlfr , 
  sv_facility_name , 
  sv_facility_primary_id , 
  sv_facility_addr_1 , 
  sv_facility_addr_2 , 
  sv_facility_city , 
  sv_facility_state , 
  sv_facility_zip , 
  sv_facility_ref_id_qlfr_1 , 
  sv_facility_secondary_id_1 , 
  sv_facility_secondary_id_2 , 
  drug_product_id_qlfr , 
  drug_product_id , 
  drug_unit_count , 
  drug_measure_unit ,
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
NULL as CLAIM_TRACKING_ID, 
MD5(GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||(SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8))||CLAIM_ID)   AS XML_MD5,
MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
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













