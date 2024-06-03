
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


//INSERT INTO LZ_EDI_837.PROF_CLAIM_PART_ERROR
//
//SELECT 
//  app_sender_code , 
//  app_reciever_code , 
//  grp_control_no , 
//  trancactset_cntl_no , 
//  impl_convention_refer , 
//  transactset_purpose_code , 
//  batch_cntl_no , 
//  transactset_create_time , 
//  transact_type_code , 
//  provider_hl_no , 
//  subscriber_hl_no , 
//  payer_hl_no , 
//  claim_id , 
//  total_claim_charge_amt , 
//  healthcareservice_location , 
//  provider_accept_assign_code , 
//  provider_benefit_auth_code , 
//  provider_patinfo_release_auth_code , 
//  delay_reason_code , 
//  health_care_code_info , 
//  CASE WHEN sv_reimbursement_rate = ''-99999999999999999999'' THEN NULL ELSE sv_reimbursement_rate END,
//  CASE WHEN sv_hcpcs_payable_amt = ''-99999999999999999999'' THEN NULL ELSE sv_hcpcs_payable_amt END,
//  sv_clm_payment_remark_code, 
//  CASE WHEN  sl_seq_num = ''99999999999999999999'' THEN NULL ELSE sl_seq_num END ,
//  product_service_id_qlfr ,
//  CASE WHEN line_item_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_charge_amt END,
//  measurement_unit, 
//  CASE WHEN  service_unit_count = ''-99999999999999999999'' THEN NULL ELSE service_unit_count END , 
//  date_time_qlfy , 
//  date_time_frmt_qlfy , 
//  service_date , 
//  cas_adj_group_code , 
//  cas_adj_reason_code , 
//  cas_adj_amt , 
//  vendor_cd , 
//  provider_sign_indicator , 
//  pat_signature_cd , 
//  related_cause_code_info , 
//  special_program_indicator , 
//  patient_amt_paid , 
//  service_auth_exception_code , 
//  clinical_lab_amendment_num , 
//  network_trace_number , 
//  medical_record_number , 
//  demonstration_project_id , 
//  health_care_additional_code_info , 
//  anesthesia_procedure_code , 
//  hc_condition_codes , 
//  payer_clm_ctrl_num , 
//  non_covered_charge_amt , 
//  diagnosis_code_pointers , 
//  emergency_indicator , 
//  family_planning_indicator , 
//  epsdt_indicator , 
//  clm_billing_note_text ,
//  transactset_create_date
//FROM IDENTIFIER(:V_TEMP_TBL)
//WHERE 
//(
//   (SV_REIMBURSEMENT_RATE   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
//   (SV_HCPCS_PAYABLE_AMT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
//   (SL_SEQ_NUM   NOT regexp ''[0-9]+'')  OR
//   (LINE_ITEM_CHARGE_AMT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
//   (SERVICE_UNIT_COUNT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  
//)
//;
//
//
//INSERT INTO IDENTIFIER(:V_SRC_TBL)
//(
//   app_sender_code , 
//  app_reciever_code , 
//  grp_control_no , 
//  trancactset_cntl_no , 
//  impl_convention_refer , 
//  transactset_purpose_code , 
//  batch_cntl_no , 
//  transactset_create_time , 
//  transact_type_code , 
//  provider_hl_no , 
//  subscriber_hl_no , 
//  payer_hl_no , 
//  claim_id , 
//  total_claim_charge_amt , 
//  healthcareservice_location , 
//  provider_accept_assign_code , 
//  provider_benefit_auth_code , 
//  provider_patinfo_release_auth_code , 
//  delay_reason_code , 
//  health_care_code_info , 
//  sv_reimbursement_rate , 
//  sv_hcpcs_payable_amt , 
//  sv_clm_payment_remark_code , 
//  sl_seq_num , 
//  product_service_id_qlfr , 
//  line_item_charge_amt , 
//  measurement_unit , 
//  service_unit_count , 
//  date_time_qlfy , 
//  date_time_frmt_qlfy , 
//  service_date , 
//  cas_adj_group_code , 
//  cas_adj_reason_code , 
//  cas_adj_amt , 
//  vendor_cd , 
//  provider_sign_indicator , 
//  pat_signature_cd , 
//  related_cause_code_info , 
//  special_program_indicator , 
//  patient_amt_paid , 
//  service_auth_exception_code , 
//  clinical_lab_amendment_num , 
//  network_trace_number , 
//  medical_record_number , 
//  demonstration_project_id , 
//  health_care_additional_code_info , 
//  anesthesia_procedure_code , 
//  hc_condition_codes , 
//  payer_clm_ctrl_num , 
//  non_covered_charge_amt , 
//  diagnosis_code_pointers , 
//  emergency_indicator , 
//  family_planning_indicator , 
//  epsdt_indicator , 
//  clm_billing_note_text ,
//  transactset_create_date ,
//  CLAIM_TRACKING_ID,
//  XML_MD5,
//  XML_HDR_MD5,
//  FILE_SOURCE,
//  FILE_NAME,
//  ISDC_CREATED_DT,
//  ISDC_UPDATED_DT
//)
// 
//SELECT DISTINCT   
//   app_sender_code , 
//  app_reciever_code , 
//  grp_control_no , 
//  trancactset_cntl_no , 
//  impl_convention_refer , 
//  transactset_purpose_code , 
//  batch_cntl_no , 
//  transactset_create_time , 
//  transact_type_code , 
//  provider_hl_no , 
//  subscriber_hl_no , 
//  payer_hl_no , 
//  claim_id , 
//  total_claim_charge_amt , 
//  healthcareservice_location , 
//  provider_accept_assign_code , 
//  provider_benefit_auth_code , 
//  provider_patinfo_release_auth_code , 
//  delay_reason_code , 
//  health_care_code_info , 
//  CASE WHEN sv_reimbursement_rate = ''-99999999999999999999'' THEN NULL ELSE sv_reimbursement_rate END,
//  CASE WHEN sv_hcpcs_payable_amt = ''-99999999999999999999'' THEN NULL ELSE sv_hcpcs_payable_amt END,
//  sv_clm_payment_remark_code , 
//  CASE WHEN  sl_seq_num = ''99999999999999999999'' THEN NULL ELSE sl_seq_num END , 
//  product_service_id_qlfr , 
//  CASE WHEN line_item_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_charge_amt END,
//  measurement_unit , 
//  CASE WHEN  service_unit_count = ''-99999999999999999999'' THEN NULL ELSE service_unit_count END , 
//  date_time_qlfy , 
//  date_time_frmt_qlfy , 
//  service_date , 
//  cas_adj_group_code , 
//  cas_adj_reason_code , 
//  cas_adj_amt , 
//  vendor_cd , 
//  provider_sign_indicator , 
//  pat_signature_cd , 
//  related_cause_code_info , 
//  special_program_indicator , 
//  patient_amt_paid , 
//  service_auth_exception_code , 
//  clinical_lab_amendment_num , 
//  network_trace_number , 
//  medical_record_number , 
//  demonstration_project_id , 
//  health_care_additional_code_info , 
//  anesthesia_procedure_code , 
//  hc_condition_codes , 
//  payer_clm_ctrl_num , 
//  non_covered_charge_amt , 
//  diagnosis_code_pointers , 
//  emergency_indicator , 
//  family_planning_indicator , 
//  epsdt_indicator , 
//  clm_billing_note_text ,
//  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
//NULL as CLAIM_TRACKING_ID, 
//  
//MD5(GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||(SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8))||CLAIM_ID)   AS XML_MD5,
//  
//MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
//||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
//''LEGACY'' AS FILE_SOURCE, 
//null as FILE_NAME, 
//CURRENT_TIMESTAMP, 
//CURRENT_TIMESTAMP
//
//FROM IDENTIFIER(:V_TEMP_TBL)
//WHERE 
//(
//   (SV_REIMBURSEMENT_RATE   regexp ''-?[0-9]+(.[0-9]+)'' )  AND
//   (SV_HCPCS_PAYABLE_AMT   regexp ''-?[0-9]+(.[0-9]+)'' )  AND
//   (SL_SEQ_NUM   regexp ''[0-9]+'')  AND
//   (LINE_ITEM_CHARGE_AMT   regexp ''-?[0-9]+(.[0-9]+)'' )  AND
//   (SERVICE_UNIT_COUNT   regexp ''-?[0-9]+(.[0-9]+)'' )  
//
//)
//;
//
//
//V_ROWS_LOADED := SQLROWCOUNT ;
//
//V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
//   
//   
//CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
//                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :TRAN_MTH, NULL, NULL);




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
,COALESCE($20, ''''99999999999999999999'''')
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
,COALESCE($37, ''''99999999999999999999'''')
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
  CASE WHEN  subscriber_type = ''99999999999999999999'' THEN NULL ELSE subscriber_type END ,
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
  CASE WHEN  other_subscriber_type = ''99999999999999999999'' THEN NULL ELSE other_subscriber_type END ,
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
  transactset_create_date



FROM IDENTIFIER(:V_TEMP_TBL)

 WHERE 
(

   (OTHER_SUBSCRIBER_TYPE   NOT regexp ''[0-9]+'')  OR
   (SUBSCRIBER_TYPE   NOT regexp ''[0-9]+'')

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
  CASE WHEN  subscriber_type = ''99999999999999999999'' THEN NULL ELSE subscriber_type END , 
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
  CASE WHEN  other_subscriber_type = ''99999999999999999999'' THEN NULL ELSE other_subscriber_type END ,
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

 WHERE 
(

   (OTHER_SUBSCRIBER_TYPE    regexp ''[0-9]+'')  AND
   (SUBSCRIBER_TYPE    regexp ''[0-9]+'')

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
