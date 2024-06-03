USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE SP_LEGACY_INST_CLAIM_CLMNBR("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
 
 
V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
 
V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';
 
V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_INST_CLAIM_CLMNBR'';
 
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
 
V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_INST_CLAIM_CLMNBR_RAW''||''_''||:TRAN_MTH;
 
V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''INST_CLAIM_CLMNBR_ERROR'';
 
V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''INST_CLAIM_CLMNBR'';

 
BEGIN
 
EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;
 
ALTER SESSION SET TIMEZONE = ''America/Chicago'';
 
V_STEP := ''STEP1'';
 
 
V_STEP_NAME := ''INST_CLAIM_CLMNBR''; 
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
 
 
V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS (SELECT *  FROM ''||V_LZ_TBL||'' WHERE 1 = 2); '';  
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
,COALESCE($18, ''''-99999999999999999999'''') 
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
,COALESCE($30, ''''-99999999999999999999'''')  
,COALESCE($31, ''''-99999999999999999999'''') 
,$32
,COALESCE($33, ''''99999999999999999999'''') 
,$34 
,$35 
,COALESCE($36, ''''-99999999999999999999'''') 
,$37 
,COALESCE($38, ''''-99999999999999999999'''') 
,$39 
,$40 
,COALESCE($41, ''''-99999999999999999999'''')  
,$42 
,COALESCE($43, ''''-99999999999999999999'''')  
,$44 
,$45 
,split($46,'''''''')
,$47 
,$48
,$49 
,$50 
,$51 
,$52 
,$53
,$54
,$55
,$56
,$57
,$58
,$59
,split($60,'''''''')
,split($61,'''''''')
,$62
,split($63,'''''''')
,split($64,'''''''')
,split($65,'''''''')
,split($66,'''''''')
,split($67,'''''''')
,split($68,'''''''')
,COALESCE($69, ''''-99999999999999999999'''') 
,COALESCE($70, ''''-99999999999999999999'''') 
,$71
,$72
,COALESCE($73, ''''-99999999999999999999'''') 
,$74
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_TILDA_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''
 
;
 
execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2;
 
 
INSERT INTO LZ_EDI_837.INST_CLAIM_CLMNBR_ERROR
SELECT  	
 ucps_clm_num , 
  ucps_clm_dt , 
  clm_type , 
  ref_id_qlfr , 
  ref_id_value , 
  member_id , 
  creat_dt , 
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
  CASE WHEN total_claim_charge_amt = ''-99999999999999999999'' THEN NULL ELSE total_claim_charge_amt END,
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
  CASE WHEN sv_reimbursement_rate = ''-99999999999999999999'' THEN NULL ELSE sv_reimbursement_rate END , 
  CASE WHEN sv_hcpcs_payable_amt = ''-99999999999999999999'' THEN NULL ELSE sv_hcpcs_payable_amt END , 
  sv_clm_payment_remark_code , 
  CASE WHEN sl_seq_num = ''99999999999999999999'' THEN NULL ELSE sl_seq_num END , 
  product_service_id , 
  product_service_id_qlfr , 
  CASE WHEN line_item_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_charge_amt END , 
  measurement_unit , 
  CASE WHEN service_unit_count = ''-99999999999999999999'' THEN NULL ELSE service_unit_count END , 
  cas_adj_group_code , 
  cas_adj_reason_code , 
  CASE WHEN cas_adj_amt = ''-99999999999999999999'' THEN NULL ELSE cas_adj_amt END , 
  delay_reason_code , 
  CASE WHEN line_item_denied_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_denied_charge_amt END  , 
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
  CASE WHEN other_payer_1_paid_amt = ''-99999999999999999999'' THEN NULL ELSE other_payer_1_paid_amt END , 
  CASE WHEN other_payer_2_paid_amt = ''-99999999999999999999'' THEN NULL ELSE other_payer_2_paid_amt END , 
  drug_product_id_qlfr , 
  drug_product_id , 
  CASE WHEN drug_unit_count = ''-99999999999999999999'' THEN NULL ELSE drug_unit_count END , 
  drug_measure_unit ,
  transactset_create_date 
    FROM IDENTIFIER(:V_TEMP_TBL)
 
WHERE 
(
(total_claim_charge_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
(SV_REIMBURSEMENT_RATE   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
(SV_HCPCS_PAYABLE_AMT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
(SL_SEQ_NUM   NOT regexp ''[0-9]+'')  OR
(LINE_ITEM_CHARGE_AMT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
(SERVICE_UNIT_COUNT   NOT regexp ''-?[0-9]+(.[0-9]+)'' ) OR
(cas_adj_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
 (line_item_denied_charge_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
(other_payer_1_paid_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
(other_payer_1_paid_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
(drug_unit_count   NOT regexp ''-?[0-9]+(.[0-9]+)'' ) 
)
;
 
INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
  ucps_clm_num , 
  ucps_clm_dt , 
  clm_type , 
  ref_id_qlfr , 
  ref_id_value , 
  member_id , 
  creat_dt , 
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
  transactset_create_date,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
SELECT DISTINCT   
  ucps_clm_num , 
  ucps_clm_dt , 
  clm_type , 
  ref_id_qlfr , 
  ref_id_value , 
  member_id , 
  creat_dt , 
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
  CASE WHEN total_claim_charge_amt = ''-99999999999999999999'' THEN NULL ELSE total_claim_charge_amt END, 
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
  CASE WHEN sv_reimbursement_rate = ''-99999999999999999999'' THEN NULL ELSE sv_reimbursement_rate END , 
  CASE WHEN sv_hcpcs_payable_amt = ''-99999999999999999999'' THEN NULL ELSE sv_hcpcs_payable_amt END , 
  sv_clm_payment_remark_code , 
  CASE WHEN sl_seq_num = ''99999999999999999999'' THEN NULL ELSE sl_seq_num END , 
  product_service_id , 
  product_service_id_qlfr , 
  CASE WHEN line_item_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_charge_amt END , 
  measurement_unit , 
  CASE WHEN service_unit_count = ''-99999999999999999999'' THEN NULL ELSE service_unit_count END , 
  cas_adj_group_code , 
  cas_adj_reason_code , 
  CASE WHEN cas_adj_amt = ''-99999999999999999999'' THEN NULL ELSE cas_adj_amt END , 
  delay_reason_code , 
  CASE WHEN line_item_denied_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_denied_charge_amt END , 
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
  CASE WHEN other_payer_1_paid_amt = ''-99999999999999999999'' THEN NULL ELSE other_payer_1_paid_amt END , 
  CASE WHEN other_payer_2_paid_amt = ''-99999999999999999999'' THEN NULL ELSE other_payer_2_paid_amt END , 
  drug_product_id_qlfr , 
  drug_product_id , 
  CASE WHEN drug_unit_count = ''-99999999999999999999'' THEN NULL ELSE drug_unit_count END , 
  drug_measure_unit ,
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP
 
FROM IDENTIFIER(:V_TEMP_TBL)
 
WHERE 
(
   (total_claim_charge_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (SV_REIMBURSEMENT_RATE   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (SV_HCPCS_PAYABLE_AMT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (SL_SEQ_NUM   NOT regexp ''[0-9]+'')  AND
   (LINE_ITEM_CHARGE_AMT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (SERVICE_UNIT_COUNT   NOT regexp ''-?[0-9]+(.[0-9]+)'' ) AND
   (cas_adj_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (line_item_denied_charge_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (other_payer_1_paid_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (other_payer_1_paid_amt   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (drug_unit_count   NOT regexp ''-?[0-9]+(.[0-9]+)'' ) 
 
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



use schema SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_LEGACY_FOXIMPORT("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_FOXIMPORT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/clm_recpt_dt=''||SUBSTR(:TRAN_MTH,1,4)||''-''||SUBSTR(:TRAN_MTH,5,2);

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_FOXIMPORT_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''FOXIMPORT_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''FOXIMPORT'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''FOXIMPORT''; 
   
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
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_PIPE_CSV'''', pattern=''''.*clm_recpt_dt=.*.000.*'''' ;''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 


INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
   clm_num , 
  clh_trk_id , 
  medcr_clm_ctl_nbr , 
  inst_prof_ind , 
  orig_mbr_nbr , 
  doc_ctl_nbr , 
  clm_stat , 
  dt_cmpltd , 
  srv_from_dt , 
  srv_to_dt , 
  fclty_cd , 
  create_dt , 
  acct_nbr ,
  clm_recpt_dt ,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  clm_num , 
  clh_trk_id , 
  medcr_clm_ctl_nbr , 
  inst_prof_ind , 
  orig_mbr_nbr , 
  doc_ctl_nbr , 
  clm_stat , 
  dt_cmpltd , 
  srv_from_dt , 
  srv_to_dt , 
  fclty_cd , 
  create_dt , 
  acct_nbr ,
  STRTOK_TO_ARRAY(clm_recpt_dt,''=/'')[5]::VARCHAR as clm_recpt_dt,
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


CREATE OR REPLACE PROCEDURE SP_LEGACY_FOX_POSTADJ_IMPORT("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_FOX_POSTADJ_IMPORT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/dt_cmpltd=''||SUBSTR(:TRAN_MTH,1,4)||''-''||SUBSTR(:TRAN_MTH,5,2);

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_FOX_POSTADJ_IMPORT_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''FOX_POSTADJ_IMPORT_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''FOX_POSTADJ_IMPORT'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''FOX_POSTADJ_IMPORT''; 
   
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
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_PIPE_CSV'''', pattern=''''.*dt_cmpltd=.*.000.*'''' ;''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 


INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
   clm_num , 
  clmsource , 
  ecsource , 
  clm_src_cd , 
  claimsuspensereason , 
  claimsuspensereasondate , 
  ecsuspensereason , 
  srv_from_dt , 
  srv_to_dt , 
  bil_ln_num , 
  chrg_amt , 
  mcare_pd_amt , 
  mcare_aprvd_amt , 
  partb_ded_amt , 
  ben_amt , 
  cpt_cd , 
  tin , 
  nat_prov_id , 
  oper_id , 
  medcr_clm_ctl_nbr , 
  clh_trk_id , 
  acct_nbr , 
  orig_mbr_nbr , 
  aarp_ded_amt , 
  aarp_copay_amt , 
  clm_note_dat , 
  pln_cd , 
  compas_pln_cd , 
  pln_ind , 
  plan , 
  oop_amt , 
  plsrv_cd , 
  srv_cd , 
  typ_cd , 
  tos_cd , 
  rend_prv_npi , 
  assgn_adj_amt , 
  clm_tot_app_amt_r_d , 
  clm_tot_ded_r_d , 
  clm_tot_amt_pd_r_d , 
  clm_tot_coinsur_amt_r_d , 
  pat_para_num , 
  doc_ctl_nbr ,
  dt_cmpltd ,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
   clm_num , 
  clmsource , 
  ecsource , 
  clm_src_cd , 
  claimsuspensereason , 
  claimsuspensereasondate , 
  ecsuspensereason , 
  srv_from_dt , 
  srv_to_dt , 
  bil_ln_num , 
  chrg_amt , 
  mcare_pd_amt , 
  mcare_aprvd_amt , 
  partb_ded_amt , 
  ben_amt , 
  cpt_cd , 
  tin , 
  nat_prov_id , 
  oper_id , 
  medcr_clm_ctl_nbr , 
  clh_trk_id , 
  acct_nbr , 
  orig_mbr_nbr , 
  aarp_ded_amt , 
  aarp_copay_amt , 
  clm_note_dat , 
  pln_cd , 
  compas_pln_cd , 
  pln_ind , 
  plan , 
  oop_amt , 
  plsrv_cd , 
  srv_cd , 
  typ_cd , 
  tos_cd , 
  rend_prv_npi , 
  assgn_adj_amt , 
  clm_tot_app_amt_r_d , 
  clm_tot_ded_r_d , 
  clm_tot_amt_pd_r_d , 
  clm_tot_coinsur_amt_r_d , 
  pat_para_num , 
  doc_ctl_nbr ,
  STRTOK_TO_ARRAY(dt_cmpltd,''=/'')[4]::VARCHAR as dt_cmpltd,
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
