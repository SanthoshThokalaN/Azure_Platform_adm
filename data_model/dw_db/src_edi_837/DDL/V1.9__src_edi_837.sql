USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE "SP_LEGACY_INST_SUBSCRIBER" 
(
  
  "PIPELINE_ID" VARCHAR(16777216), 
  "PIPELINE_NAME" VARCHAR(16777216), 
  "TRAN_MTH" VARCHAR(16777216), 
  "DB_NAME" VARCHAR(16777216), 
  "UTIL_SC" VARCHAR(16777216), 
  "SRC_SC" VARCHAR(16777216), 
  "TGT_SC" VARCHAR(16777216),
  "WH" VARCHAR(16777216), 
   STAGE VARCHAR

)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||'.'||COALESCE(:UTIL_SC, 'UTIL')||'.SP_PROCESS_RUN_LOGS_DTL';

V_PROCESS_NAME   VARCHAR DEFAULT 'LEGACY_LOAD';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  'LEGACY_INST_SUBSCRIBER';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||'/transactset_create_date='||:TRAN_MTH;

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||'.'||'TMP_INST_SUBSCRIBER_RAW'||'_'||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||'.'||'INST_SUBSCRIBER_RAW';

V_SRC_TBL   VARCHAR :=  :TGT_SC||'.'||'INST_SUBSCRIBER';
 

BEGIN

EXECUTE IMMEDIATE 'USE WAREHOUSE '||:WH;

ALTER SESSION SET TIMEZONE = 'America/Chicago';

V_STEP := 'STEP1';




V_STEP_NAME := 'INST_SUBSCRIBER'; 
   
V_START_TIME := CONVERT_TIMEZONE('America/Chicago', CURRENT_TIMESTAMP());


V_QUERY1 := 'CREATE OR REPLACE TEMPORARY TABLE '||:V_TEMP_TBL||' AS (SELECT * FROM '||V_LZ_TBL||' WHERE 1 = 2); ';  
 
V_QUERY2 := ' 

COPY INTO '||:V_TEMP_TBL||' FROM  (
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
,$49 
,$50 
,metadata$filename
from '||:V_STAGE||' ) file_format = ''UTIL.FF_SOH_CSV'', pattern=''.*transactset_create_date=.*.000.*'' ;'

;

execute immediate 'USE SCHEMA '||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 

INSERT INTO LZ_EDI_837.INST_SUBSCRIBER_ERROR
SELECT * FROM IDENTIFIER(:V_TEMP_TBL)
WHERE (SUBSCRIBER_TYPE NOT regexp '[0-9]' AND SUBSCRIBER_TYPE IS NOT NULL)
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
  other_payer_responsibility_code , 
  other_subscriber_relationship_code , 
  other_subscriber_policy_number , 
  other_subscriber_plan_name , 
  other_clm_filling_ind_cd , 
  other_subscriber_type , 
  other_subscriber_name_last , 
  other_subscriber_name_first , 
  other_subscriber_name_middle , 
  other_subscriber_name_suffix , 
  other_subscriber_id_qlfr , 
  other_subscriber_id , 
  other_subscriber_address_1 , 
  other_subscriber_address_2 , 
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
     app_sender_code , 
  app_reciever_code , 
  grp_control_no , 
  trancactset_cntl_no , 
  impl_convention_refer , 
  transactset_purpose_code , 
  batch_cntl_no , 
  transactset_create_time , 
  transact_type_code , 
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
  other_payer_responsibility_code , 
  other_subscriber_relationship_code , 
  other_subscriber_policy_number , 
  other_subscriber_plan_name , 
  other_clm_filling_ind_cd , 
  other_subscriber_type , 
  other_subscriber_name_last , 
  other_subscriber_name_first , 
  other_subscriber_name_middle , 
  other_subscriber_name_suffix , 
  other_subscriber_id_qlfr , 
  other_subscriber_id , 
  other_subscriber_address_1 , 
  other_subscriber_address_2 , 
  other_subscriber_city , 
  other_subscriber_state , 
  other_subscriber_zip ,
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, 'transactset_create_date=')+24, 8) AS transactset_create_date ,
NULL as CLAIM_TRACKING_ID, 
NULL   AS XML_MD5,
MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, 'transactset_create_date=')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
'LEGACY' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)
WHERE (SUBSCRIBER_TYPE regexp '[0-9]' OR SUBSCRIBER_TYPE IS NULL)

;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, 'PRE_CLM', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), 'SUCCESS', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :TRAN_MTH, NULL, NULL);


UPDATE UTIL.LEGACY_LOAD SET LOAD_INDC = 'Y' WHERE TABLE_NAME = :V_SUB_PROCESS_NAME AND TRAN_MTH = :TRAN_MTH;

EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, 'PRE_CLM', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), 'FAILED', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

$$

;





USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_LEGACY_INST_CLAIM_PART"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_INST_CLAIM_PART'';

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

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_INST_CLAIM_PART_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''INST_CLAIM_PART_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''INST_CLAIM_PART'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';




V_STEP_NAME := ''INST_CLAIM_PART''; 
   
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
,split($39,'''''''') 
,$40 
,$41 
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
,split($53,'''''''') 
,split($54,'''''''')
,$55
,split($56,'''''''') 
,split($57,'''''''') 
,split($58,'''''''')
,split($59,'''''''') 
,split($60,'''''''') 
,split($61,'''''''') 
,$62 
,$63 
,$64 
,$65 
,$66 
,$67 
,$68 
,$69 
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_SOH_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''

;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 

INSERT INTO LZ_EDI_837.INST_CLAIM_PART_ERROR
SELECT * FROM IDENTIFIER(:V_TEMP_TBL)
WHERE 
(
    (TOTAL_CLAIM_CHARGE_AMT  NOT regexp ''[0-9]'' AND TOTAL_CLAIM_CHARGE_AMT IS NOT NULL)  AND 
    (SV_REIMBURSEMENT_RATE  NOT regexp ''[0-9]'' AND SV_REIMBURSEMENT_RATE IS NOT NULL)  AND
    (SV_HCPCS_PAYABLE_AMT  NOT regexp ''[0-9]'' AND SV_HCPCS_PAYABLE_AMT IS NOT NULL)  AND
    (SL_SEQ_NUM  NOT regexp ''[0-9]'' AND SL_SEQ_NUM IS NOT NULL)  AND
    (LINE_ITEM_CHARGE_AMT  NOT regexp ''[0-9]'' AND LINE_ITEM_CHARGE_AMT IS NOT NULL)  AND
    (SERVICE_UNIT_COUNT  NOT regexp ''[0-9]'' AND SERVICE_UNIT_COUNT IS NOT NULL)  AND
    (CAS_ADJ_AMT  NOT regexp ''[0-9]'' AND CAS_ADJ_AMT IS NOT NULL)  AND
    (OTHER_PAYER_1_PAID_AMT  NOT regexp ''[0-9]'' AND OTHER_PAYER_1_PAID_AMT IS NOT NULL)  AND 
    (OTHER_PAYER_2_PAID_AMT  NOT regexp ''[0-9]'' AND OTHER_PAYER_2_PAID_AMT IS NOT NULL)  AND       
    (DRUG_UNIT_COUNT  NOT regexp ''[0-9]'' AND DRUG_UNIT_COUNT IS NOT NULL)  
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
  claim_id , 
  total_claim_charge_amt , 
  healthcareservice_location , 
  provider_accept_assign_code , 
  provider_benefit_auth_code , 
  provider_patinfo_release_auth_code , 
  date_time_qlfy , 
  date_time_frmt_qlfy , 
  statement_date , 
  admit_type_code , 
  admit_source_code , 
  patient_status_code , 
  health_care_code_info , 
  sv_reimbursement_rate , 
  sv_hcpcs_payable_amt , 
  sv_clm_payment_remark_code , 
  sl_seq_num , 
  product_service_id , 
  product_service_id_qlfr , 
  line_item_charge_amt , 
  measurement_unit , 
  service_unit_count , 
  cas_adj_group_code , 
  cas_adj_reason_code , 
  cas_adj_amt , 
  delay_reason_code , 
  line_item_denied_charge_amt , 
  network_trace_number , 
  principal_procedure_info , 
  hc_condition_codes , 
  clm_lab_facility_name , 
  clm_lab_facility_id , 
  clm_lab_facility_addr1 , 
  clm_lab_facility_addr2 , 
  clm_lab_facility_city , 
  clm_lab_facility_state , 
  clm_lab_facility_zip , 
  clm_lab_facility_ref_id_qlfr , 
  clm_lab_facility_ref_id , 
  medical_record_number , 
  clm_note_text , 
  clm_billing_note_text , 
  clm_admitting_diagnosis_cd , 
  patient_reason_for_visit_cd , 
  external_cause_of_injury , 
  diagnosis_related_grp_info , 
  other_diagnosis_cd_info , 
  other_procedure_info , 
  occurrence_span_info , 
  occurrence_info , 
  value_info , 
  treatment_cd_info , 
  other_payer_1_paid_amt , 
  other_payer_2_paid_amt , 
  drug_product_id_qlfr , 
  drug_product_id , 
  drug_unit_count , 
  drug_measure_unit , 
  payer_clm_ctrl_num , 
  clm_note_ref_cd ,
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
  claim_id , 
  total_claim_charge_amt , 
  healthcareservice_location , 
  provider_accept_assign_code , 
  provider_benefit_auth_code , 
  provider_patinfo_release_auth_code , 
  date_time_qlfy , 
  date_time_frmt_qlfy , 
  statement_date , 
  admit_type_code , 
  admit_source_code , 
  patient_status_code , 
  health_care_code_info , 
  sv_reimbursement_rate , 
  sv_hcpcs_payable_amt , 
  sv_clm_payment_remark_code , 
  sl_seq_num , 
  product_service_id , 
  product_service_id_qlfr , 
  line_item_charge_amt , 
  measurement_unit , 
  service_unit_count , 
  cas_adj_group_code , 
  cas_adj_reason_code , 
  cas_adj_amt , 
  delay_reason_code , 
  line_item_denied_charge_amt , 
  network_trace_number , 
  principal_procedure_info , 
  hc_condition_codes , 
  clm_lab_facility_name , 
  clm_lab_facility_id , 
  clm_lab_facility_addr1 , 
  clm_lab_facility_addr2 , 
  clm_lab_facility_city , 
  clm_lab_facility_state , 
  clm_lab_facility_zip , 
  clm_lab_facility_ref_id_qlfr , 
  clm_lab_facility_ref_id , 
  medical_record_number , 
  clm_note_text , 
  clm_billing_note_text , 
  clm_admitting_diagnosis_cd , 
  patient_reason_for_visit_cd , 
  external_cause_of_injury , 
  diagnosis_related_grp_info , 
  other_diagnosis_cd_info , 
  other_procedure_info , 
  occurrence_span_info , 
  occurrence_info , 
  value_info , 
  treatment_cd_info , 
  other_payer_1_paid_amt , 
  other_payer_2_paid_amt , 
  drug_product_id_qlfr , 
  drug_product_id , 
  drug_unit_count , 
  drug_measure_unit , 
  payer_clm_ctrl_num , 
  clm_note_ref_cd ,
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
    (TOTAL_CLAIM_CHARGE_AMT  regexp ''[0-9]'' OR TOTAL_CLAIM_CHARGE_AMT IS NULL)  OR
    (SV_REIMBURSEMENT_RATE  regexp ''[0-9]'' OR SV_REIMBURSEMENT_RATE IS NULL)  OR
    (SV_HCPCS_PAYABLE_AMT  regexp ''[0-9]'' OR SV_HCPCS_PAYABLE_AMT IS NULL)  OR
    (SL_SEQ_NUM  regexp ''[0-9]'' OR SL_SEQ_NUM IS NULL)  OR
    (LINE_ITEM_CHARGE_AMT  regexp ''[0-9]'' OR LINE_ITEM_CHARGE_AMT IS NULL)  OR
    (SERVICE_UNIT_COUNT  regexp ''[0-9]'' OR SERVICE_UNIT_COUNT IS NULL)  OR
    (CAS_ADJ_AMT  regexp ''[0-9]'' OR CAS_ADJ_AMT IS NULL)  OR
    (OTHER_PAYER_1_PAID_AMT  regexp ''[0-9]'' OR OTHER_PAYER_1_PAID_AMT IS NULL)  OR 
    (OTHER_PAYER_2_PAID_AMT  regexp ''[0-9]'' OR OTHER_PAYER_2_PAID_AMT IS NULL)  OR       
    (DRUG_UNIT_COUNT  regexp ''[0-9]'' OR DRUG_UNIT_COUNT IS NULL)  
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
