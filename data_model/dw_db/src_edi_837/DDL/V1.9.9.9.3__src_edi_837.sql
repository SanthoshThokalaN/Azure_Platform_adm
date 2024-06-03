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
   (SV_REIMBURSEMENT_RATE   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
   (SV_HCPCS_PAYABLE_AMT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
   (SL_SEQ_NUM   NOT regexp ''[0-9]+'')  OR
   (LINE_ITEM_CHARGE_AMT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  OR
   (SERVICE_UNIT_COUNT   NOT regexp ''-?[0-9]+(.[0-9]+)'' )  
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
  
MD5(GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||(SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8))||CLAIM_ID)   AS XML_MD5,
  
MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)
WHERE 
(
   (SV_REIMBURSEMENT_RATE   regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (SV_HCPCS_PAYABLE_AMT   regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (SL_SEQ_NUM   regexp ''[0-9]+'')  AND
   (LINE_ITEM_CHARGE_AMT   regexp ''-?[0-9]+(.[0-9]+)'' )  AND
   (SERVICE_UNIT_COUNT   regexp ''-?[0-9]+(.[0-9]+)'' )  

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



CREATE or REPLACE  PROCEDURE SRC_EDI_837.SP_LEGACY_PROVIDER_SPIKE_RAW("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROVIDER_SPIKE_RAW'';

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

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROVIDER_SPIKE_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROVIDER_SPIKE_RAW_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROVIDER_SPIKE_RAW'';

V_ERROR_TBL VARCHAR :=  :SRC_SC||''.''||''provider_spike_raw_error'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''PROVIDER_SPIKE_RAW''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS (SELECT * FROM ''||:V_ERROR_TBL||'' WHERE 1 = 2); '';  
 
V_QUERY2 := '' 

COPY INTO ''||:V_TEMP_TBL||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,COALESCE($7, ''''99999999999999999999'''') 
,COALESCE($8, ''''99999999999999999999'''') 
,$9 
,COALESCE($10, ''''-99999999999999999999'''') 
,COALESCE($11, ''''99999999999999999999'''') 
,$12
,$13 
,$14
,$15
,$16
,metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_PIPE_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''
;




execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 

INSERT INTO IDENTIFIER(:V_ERROR_TBL) 
SELECT DISTINCT   
service_date ,          
subscriber_id  ,        
providernpi     ,       
npisource        ,      
billprovidertaxid ,     
billproviderstates ,    
CASE WHEN months = ''99999999999999999999'' THEN NULL ELSE months END,
CASE WHEN years = ''99999999999999999999'' THEN NULL ELSE years END,
claim_id,
CASE WHEN line_item_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_charge_amt END,
CASE WHEN bill_line_num = ''99999999999999999999'' THEN NULL ELSE bill_line_num END,
grp_control_no         ,
trancactset_cntl_no    ,
provider_hl_no         ,
subscriber_hl_no       ,
payer_hl_no            ,
SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date
FROM IDENTIFIER(:V_TEMP_TBL)
where (
   
   (months  NOT regexp ''[0-9]+'')  OR
   (years  NOT regexp ''[0-9]+'')  OR   
   (line_item_charge_amt  NOT regexp ''-?[0-9]+(\.[0-9]+)'' )  OR
   (bill_line_num NOT regexp ''[0-9]+'') 
  
)
;

INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
 service_date ,          
subscriber_id  ,        
providernpi     ,       
npisource        ,      
billprovidertaxid ,     
billproviderstates ,    
months              ,   
years                ,  
claim_id              , 
line_item_charge_amt   ,
bill_line_num          ,
grp_control_no         ,
trancactset_cntl_no    ,
provider_hl_no         ,
subscriber_hl_no       ,
payer_hl_no            ,
transactset_create_date,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
   service_date ,          
subscriber_id  ,        
providernpi     ,       
npisource        ,      
billprovidertaxid ,     
billproviderstates ,    
CASE WHEN months = ''99999999999999999999'' THEN NULL ELSE months END,
CASE WHEN years = ''99999999999999999999'' THEN NULL ELSE years END,
claim_id,
CASE WHEN line_item_charge_amt = ''-99999999999999999999'' THEN NULL ELSE line_item_charge_amt END,
CASE WHEN bill_line_num = ''99999999999999999999'' THEN NULL ELSE bill_line_num END,
grp_control_no         ,
trancactset_cntl_no    ,
provider_hl_no         ,
subscriber_hl_no       ,
payer_hl_no            ,
SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)

where (
   
   (months   regexp ''[0-9]+'')  AND
   (years   regexp ''[0-9]+'')  AND   
   (line_item_charge_amt  regexp ''-?[0-9]+(\.[0-9]+)'' )  AND
   (bill_line_num  regexp ''[0-9]+'') 
  
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



CREATE or replace PROCEDURE SRC_EDI_837.SP_LEGACY_TAXONOMY_MISMATCH_RAW ("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), 
                                                                         "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), 
                                                                         "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_TAXONOMY_MISMATCH_RAW'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/claim_pd_date=''||:TRAN_MTH;

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_TAXONOMY_MISMATCH_RAW''||''_''||:TRAN_MTH;

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''TAXONOMY_MISMATCH_RAW'';
 
V_ERROR_TBL VARCHAR :=  :SRC_SC||''.''||''TAXONOMY_MISMATCH_RAW_ERROR'';

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''TAXONOMY_MISMATCH_RAW''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS (SELECT * FROM ''||:V_ERROR_TBL||'' WHERE 1 = 2); '';  
 
V_QUERY2 := '' 

COPY INTO ''||:V_TEMP_TBL||'' FROM  (
select 
 $1 ,
COALESCE($2, ''''99999999999999999999'''') ,
$3 ,
COALESCE($4, ''''-99999999999999999999'''') ,
$5 ,
$6 ,
$7 ,
COALESCE($8, ''''-99999999999999999999'''') ,

metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_PIPE_CSV'''', pattern=''''.*claim_pd_date=.*.000.*'''' ;''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 


INSERT INTO IDENTIFIER(:V_ERROR_TBL) 
SELECT DISTINCT   
  ucps_clm_num       ,

  CASE WHEN bill_line = ''99999999999999999999'' THEN NULL ELSE bill_line END,
  
  cpt_codes          ,
  
    CASE WHEN med_approved_amount = ''-99999999999999999999'' THEN NULL ELSE med_approved_amount END,

  taxonomy_codes     ,
  provider_tax_id    ,
  provider_npi       ,
      CASE WHEN adj_ben_amt = ''-99999999999999999999'' THEN NULL ELSE adj_ben_amt END,

SUBSTR(claim_pd_date, REGEXP_INSTR(claim_pd_date, ''claim_pd_date='')+14, 8) AS claim_pd_date

FROM IDENTIFIER(:V_TEMP_TBL)
where (
   
   (BILL_LINE  NOT regexp ''[0-9]+'')  OR
   (MED_APPROVED_AMOUNT  NOT regexp ''-?[0-9]+(\.[0-9]+)'' ) OR
    (ADJ_BEN_AMT  NOT regexp ''-?[0-9]+(\.[0-9]+)'' )
)
;




INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
 ucps_clm_num       ,
  bill_line          ,
  cpt_codes          ,
  med_approved_amount,
  taxonomy_codes     ,
  provider_tax_id    ,
  provider_npi       ,
  adj_ben_amt        ,
  claim_pd_date  ,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  ucps_clm_num       ,

  CASE WHEN bill_line = ''99999999999999999999'' THEN NULL ELSE bill_line END,
  
  cpt_codes          ,
  
    CASE WHEN med_approved_amount = ''-99999999999999999999'' THEN NULL ELSE med_approved_amount END,

  taxonomy_codes     ,
  provider_tax_id    ,
  provider_npi       ,
      CASE WHEN adj_ben_amt = ''-99999999999999999999'' THEN NULL ELSE adj_ben_amt END,

SUBSTR(claim_pd_date, REGEXP_INSTR(claim_pd_date, ''claim_pd_date='')+14, 8) AS claim_pd_date ,
CURRENT_TIMESTAMP ,
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)

where (
   
   (BILL_LINE   regexp ''[0-9]+'')  AND
   (MED_APPROVED_AMOUNT   regexp ''-?[0-9]+(\.[0-9]+)'' )  AND
    (ADJ_BEN_AMT   regexp ''-?[0-9]+(\.[0-9]+)'' )
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
  
