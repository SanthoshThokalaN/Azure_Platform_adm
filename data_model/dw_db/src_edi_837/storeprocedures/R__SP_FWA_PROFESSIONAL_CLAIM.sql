USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_FWA_PROFESSIONAL_CLAIM("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '

DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME             VARCHAR        default ''FWA_PERFORMANT'';
V_SUB_PROCESS_NAME         VARCHAR        default ''FWA_PROFESSIONAL_CLAIM'';
V_STEP                     VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER; 
V_ROWS_LOADED              INTEGER; 
V_MESSAGE                  VARCHAR;
V_LAST_QUERY_ID            VARCHAR; 
V_STAGE_QUERY1              VARCHAR;
V_STAGE_QUERY2              VARCHAR;
V_PREV_MONTH_START         VARCHAR;
V_PREV_MONTH_END           VARCHAR;
V_CLAIM_RECEIPT_START_DATE VARCHAR;
V_CLAIM_RECEIPT_END_DATE   VARCHAR;




V_TMP_FWA_PROFESSIONAL_CLAIM               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_FWA_PROFESSIONAL_CLAIM'';

V_TMP_DB2IMPORT_CLM_FILTERED_NEW          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_DB2IMPORT_CLM_FILTERED_NEW'';
V_TMP_CLAIM          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_CLAIM'';
V_TMP_PRVALL          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PRVALL'';
V_TMP_AMBLNC          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_AMBLNC'';



V_PROF_CLAIM_PART          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLAIM_PART'';
V_PROF_CLM_SV_DATES        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLM_SV_DATES'';
V_PROF_AMBLNC_INFO         VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_AMBLNC_INFO'';
V_PROF_PROVIDER             VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PROVIDER'';
V_PROF_SUBSCRIBER          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_SUBSCRIBER'';
V_PROF_PATIENT             VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PATIENT'';
V_PROF_PROVIDER_ALL        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PROVIDER_ALL'';
V_CH_VIEW                  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.CH_VIEW'';



BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_PREV_MONTH_START := (SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYYMMDD''));
V_PREV_MONTH_END :=   (SELECT TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE))), ''YYYYMMDD''));

V_CLAIM_RECEIPT_START_DATE :=  (SELECT TO_VARCHAR(DATEADD(MONTH, -3, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYYMMDD''));
V_CLAIM_RECEIPT_END_DATE :=  (SELECT TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE))), ''YYYYMMDD''));



V_STEP := ''STEP1'';
   
V_STEP_NAME := ''Load TMP_DB2IMPORT_CLM_FILTERED_NEW'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());



CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_TMP_DB2IMPORT_CLM_FILTERED_NEW) AS
SELECT db2.* FROM IDENTIFIER(:V_CH_VIEW) db2 WHERE TO_NUMBER(TO_VARCHAR(db2.clm_recept_dt::date, ''YYYYMMDD'')) BETWEEN TO_NUMBER(:V_CLAIM_RECEIPT_START_DATE) AND TO_NUMBER(:V_CLAIM_RECEIPT_END_DATE);

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_NEW)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   



V_STEP := ''STEP2'';
   
V_STEP_NAME := ''Load TMP_CLAIM'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_TMP_CLAIM) AS 
SELECT DISTINCT 
clmbase.grp_control_no,
clmbase.trancactset_cntl_no,
clmbase.provider_hl_no,
clmbase.subscriber_hl_no,
clmbase.payer_hl_no,
clmbase.claim_id,
clmbase.total_claim_charge_amt,
clmbase.healthcareservice_location,
clmbase.provider_accept_assign_code,
clmbase.provider_benefit_auth_code,
clmbase.provider_patinfo_release_auth_code,
clmbase.pat_signature_cd,
clmbase.related_cause_code_info,
clmbase.special_program_indicator,
clmbase.delay_reason_code,
clmbase.patient_amt_paid,
clmbase.service_auth_exception_code,
clmbase.clinical_lab_amendment_num,
clmbase.medical_record_number,
clmbase.demonstration_project_id,
clmbase.payer_clm_ctrl_num,
clmbase.network_trace_number,
ARRAY_TO_STRING(clmbase.hc_condition_codes,'''') as hc_condition_codes,
clmbase.health_care_code_info,
ARRAY_TO_STRING(clmbase.health_care_additional_code_info,'''') as health_care_additional_code_info,
clmbase.anesthesia_procedure_code,
clmbase.vendor_cd,
clmbase.transactset_create_date,
SPLIT(clmbase.product_service_id_qlfr,'':'')[1]::string as procedure_cd,
MONTH(TO_DATE(clmbase.transactset_create_date, ''yyyymmdd'')) as dl_clm_month,
YEAR(TO_DATE(clmbase.transactset_create_date, ''yyyymmdd'')) as dl_clm_year,
clmbase.non_covered_charge_amt,
clmbase.sv_reimbursement_rate,
clmbase.sv_hcpcs_payable_amt,
clmbase.sv_clm_payment_remark_code,
clmbase.xml_md5 
FROM IDENTIFIER (:V_PROF_CLAIM_PART) clmbase
WHERE TO_NUMBER(clmbase.transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END);


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_CLAIM)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);



V_STEP := ''STEP3'';
   
V_STEP_NAME := ''Load TMP_PRVALL'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_TMP_PRVALL) AS

SELECT DISTINCT
prvall.grp_control_no,
prvall.trancactset_cntl_no,
prvall.provider_hl_no,
prvall.subscriber_hl_no,
prvall.payer_hl_no,
prvall.transactset_create_date,
prvall.claim_id,
prvall.clm_supervising_prv_map[''prv_type_qlfr'']::string as supervising_prv_type_qlfr,
prvall.clm_supervising_prv_map[''prv_name_last'']::string as supervising_prv_name_last,
prvall.clm_supervising_prv_map[''prv_name_first'']::string as supervising_prv_name_first,
prvall.clm_supervising_prv_map[''prv_name_middle'']::string as supervising_prv_name_middle,
prvall.clm_supervising_prv_map[''prv_name_suffix'']::string as supervising_prv_name_suffix,
prvall.clm_supervising_prv_map[''prv_id'']::string as supervising_prv_id,
prvall.clm_supervising_prv_map[''prv_ref_id_qlfy'']::string as supervising_prv_ref_id_qlfy,
prvall.clm_supervising_prv_map[''prv_second_id'']::string as supervising_prv_second_id,
prvall.clm_referring_prv_map[''prv_type_qlfr'']::string as referring_type_qlfr,
prvall.clm_referring_prv_map[''prv_name_last'']::string as referring_prv_name_last,
prvall.clm_referring_prv_map[''prv_name_first'']::string as referring_prv_name_first,
prvall.clm_referring_prv_map[''prv_name_middle'']::string as referring_prv_name_middle,
prvall.clm_referring_prv_map[''prv_name_suffix'']::string as referring_prv_name_suffix,
prvall.clm_referring_prv_map[''prv_id'']::string as referring_prv_id,
prvall.clm_referring_prv_map[''prv_ref_id_qlfy'']::string as referring_prv_ref_id_qlfy,
prvall.clm_referring_prv_map[''prv_second_id'']::string as referring_prv_second_id,
prvall.clm_rendering_prv_map[''prv_type_qlfr'']::string as rendering_prv_type_qlfr,
prvall.clm_rendering_prv_map[''prv_name_last'']::string as rendering_prv_name_last,
prvall.clm_rendering_prv_map[''prv_name_first'']::string as rendering_prv_name_first,
prvall.clm_rendering_prv_map[''prv_name_middle'']::string as rendering_prv_name_middle,
prvall.clm_rendering_prv_map[''prv_name_suffix'']::string as rendering_prv_name_suffix,
prvall.clm_rendering_prv_map[''prv_id'']::string as rendering_prv_id,
prvall.clm_rendering_prv_map[''prv_ref_id_qlfy'']::string as rendering_prv_ref_id_qlfy,
prvall.clm_rendering_prv_map[''prv_second_id'']::string as rendering_prv_second_id,
prvall.clm_rendering_prv_map[''prv_speciality_id_qlfr'']::string as rendering_speciality_id_qlfr,
prvall.clm_rendering_prv_map[''prv_speciality_tax_code'']::string as rendering_prv_speciality_tax_code,
prvall.clm_rendering_prv_map[''other_payer_ren_prv_type'']::string as rendering_other_payer_ren_prv_type,
prvall.xml_md5
FROM IDENTIFIER (:V_PROF_PROVIDER_ALL) prvall 
WHERE TO_NUMBER(prvall.transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END);


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PRVALL)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);



V_STEP := ''STEP4'';
   
V_STEP_NAME := ''Load TMP_AMBLNC'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_TMP_AMBLNC) AS
SELECT DISTINCT
amblnc.grp_control_no,
amblnc.trancactset_cntl_no,
amblnc.provider_hl_no,
amblnc.subscriber_hl_no,
amblnc.payer_hl_no,
amblnc.transactset_create_date,
amblnc.claim_id,
amblnc.clm_amblnc_pickup_addr_1 as clm_amblnc_pickup_addr_1,
amblnc.clm_amblnc_pickup_addr_2 as clm_amblnc_pickup_addr_2,
amblnc.clm_amblnc_pickup_city as clm_amblnc_pickup_city,
amblnc.clm_amblnc_pickup_state as clm_amblnc_pickup_state,
amblnc.clm_amblnc_pickup_zip as clm_amblnc_pickup_zip,
amblnc.clm_amblnc_dropoff_location as clm_amblnc_dropoff_location,
amblnc.clm_amblnc_dropoff_addr_1 as clm_amblnc_dropoff_addr_1,
amblnc.clm_amblnc_dropoff_addr_2 as clm_amblnc_dropoff_addr_2,
amblnc.clm_amblnc_dropoff_city as clm_amblnc_dropoff_city,
amblnc.clm_amblnc_dropoff_state as clm_amblnc_dropoff_state,
amblnc.clm_amblnc_dropoff_zip as clm_amblnc_dropoff_zip
FROM IDENTIFIER (:V_PROF_AMBLNC_INFO) amblnc 
WHERE TO_NUMBER(amblnc.transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END);


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_AMBLNC)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


                                 
V_STEP := ''STEP5'';
   
V_STEP_NAME := ''Load TMP_FWA_PROFESSIONAL_CLAIM'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_FWA_PROFESSIONAL_CLAIM) AS
SELECT DISTINCT claim.* from (
SELECT DISTINCT
db2.clm_num as "ucps_clm_num",
db2.clm_recept_dt as "exctract_key",
prv.provider_type as "billing_provider_type",
prv.provider_name as "billing_provider_name",
prv.provider_name_first as "billing_provider_name_first",
prv.provider_name_middle as "billing_provider_name_middle",
prv.name_prefix as "billing_provider_name_prefix",
prv.name_suffix as "billing_provider_name_suffix",
prv.provider_id as "billing_provider_id",
prv.provider_tax_code as "billing_provider_tax_code",
prv.payto_ent_type_qlfy as "payto_ent_type_qlfy",
prv.payto_address_1 as "payto_address_1",
prv.payto_address_2 as "payto_address_2",
prv.payto_city as "payto_city",
prv.payto_state as "payto_state",
prv.payto_zip as "payto_zip",
pat.pat_type_qlfr as "patient_type_qlfr",
pat.pat_name_last as "patient_name_last",
pat.pat_name_first as "patient_name_first",
pat.pat_name_middle as "patient_name_middle",
pat.pat_name_suffix as "patient_name_suffix",
pat.pat_address_1 as "patient_address_1",
pat.pat_address_2 as "patient_address_2",
pat.pat_city as "patient_city",
pat.pat_state as "patient_state",
pat.pat_zip as "patient_zip",
pat.pat_dateofbirth as "patient_dateofbirth",
pat.pat_gendercode as "patient_gendercode",
pat.pat_death_dt as "patient_death_dt",
pat.pat_weight_measure_unit as "patient_weight_measure_unit",
pat.pat_weight as "patient_weight" ,
pat.pat_pregnancy_indicator as "patient_pregnancy_indicator" ,
pat.pat_insurer_relationship_cd as "patient_insurer_relationship_cd",
pat.pat_dep_death_dt as "patient_dependent_death_dt"  ,
pat.pat_dep_weight_measure_unit as "patient_dependent_weight_measure_unit" ,
pat.pat_dep_weight as "patient_dependent_weight",
pat.pat_dep_pregnancy_indicator as "patient_dependent_pregnancy_indicator",
mem.payer_responsibility_code as "payer_responsibility_code",
mem.subscriber_relationship_code as "subscriber_relationship_code",
mem.subscriber_policy_number as "subscriber_policy_number",
mem.subscriber_plan_name as "subscriber_plan_name",
mem.subscriber_insurance_type as "subscriber_insurance_type",
mem.claim_type as "claim_filing_indicator_code",
mem.subscriber_type as "subscriber_type",
mem.subscriber_name_last as "subscriber_name_last",
mem.subscriber_name_first as "subscriber_name_first",
mem.subscriber_name_middle as "subscriber_name_middle",
mem.name_prefix as "subscriber_name_prefix",
mem.name_suffix as "subscriber_name_suffix",
mem.subscriber_id_qualifier as "subscriber_id_qualifier",
mem.subscriber_id as "subscriber_id",
mem.subscriber_address1 as "subscriber_address1",
mem.subscriber_address2 as "subscriber_address2",
mem.subscriber_city as "subscriber_city",
mem.subscriber_state as "subscriber_state",
mem.subscriber_postalcode as "subscriber_postalcode",
mem.subscriber_dateofbirth as "subscriber_dateofbirth",
mem.subscriber_gendercode as "subscriber_gendercode",
mem.subscriber_suplemental_id_qlfr as "subscriber_suplemental_id_qlfr",
mem.subscriber_sumplemental_id as "subscriber_suplemental_id",
clm.claim_id as "patient_control_number",
clm.total_claim_charge_amt as "total_claim_charge_amt",
SPLIT(clm.healthcareservice_location,'':'')[0]::string as "place_of_service_cd",
SPLIT(clm.healthcareservice_location,'':'')[1]::string as "facility_cd_qlfr",
SPLIT(clm.healthcareservice_location,'':'')[2]::string as "claim_frequency_type_code",
clm.provider_accept_assign_code as "provider_accept_assign_code",
clm.provider_benefit_auth_code as "provider_benefit_auth_code",
clm.provider_patinfo_release_auth_code as "provider_patinfo_release_auth_code",
clm.pat_signature_cd as "pat_signature_cd",
SPLIT(clm.related_cause_code_info,'':'')[0]::string as "related_cause_code",
SPLIT(clm.related_cause_code_info,'':'')[3]::string as "autoaccident_state",
SPLIT(clm.related_cause_code_info,'':'')[4]::string as "autoaccident_country",
clm.special_program_indicator as "special_program_indicator",
clm.delay_reason_code as "delay_reason_code",
clm.patient_amt_paid as "patient_amt_paid",
clm.service_auth_exception_code as "service_auth_exception_code",
clm.clinical_lab_amendment_num as "clinical_lab_amendment_num",
clm.medical_record_number as "medical_record_number",
clm.demonstration_project_id as "demonstration_project_id",
clm.payer_clm_ctrl_num as "payer_clm_ctrl_num",
clm.network_trace_number as "network_trace_number",
concat_ws(''*'',clm.hc_condition_codes) as "healthcare_condition_codes",
clm.health_care_code_info as "healthcare_diagnosis_codes",
concat_ws(''*'',clm.health_care_additional_code_info) as "health_care_additional_codes",
clm.anesthesia_procedure_code as "anesthesia_procedure_code",
clm.vendor_cd as "clm_submit_vendor",
dates.current_illness_injury_date as "current_illness_injury_date",
dates.initial_treatment_date as "initial_treatment_date",
dates.treatment_therapy_date as "treatment_therapy_date",
dates.acute_manifestation_date as "acute_manifestation_date",
dates.accident_date as "accident_date",
dates.hospitalized_admission_date as "hospitalized_admission_date",
dates.hospitalized_discharge_date as "hospitalized_discharge_date",
prvall.supervising_prv_type_qlfr as "supervising_prv_type_qlfr",
prvall.supervising_prv_name_last as "supervising_prv_name_last",
prvall.supervising_prv_name_first as "supervising_prv_name_first",
prvall.supervising_prv_name_middle as "supervising_prv_name_middle",
prvall.supervising_prv_name_suffix as "supervising_prv_name_suffix",
prvall.supervising_prv_id as "supervising_prv_id",
prvall.supervising_prv_ref_id_qlfy as "supervising_prv_ref_id_qlfy",
prvall.supervising_prv_second_id as "supervising_prv_second_id",
prvall.referring_type_qlfr as "referring_type_qlfr",
prvall.referring_prv_name_last as "referring_prv_name_last",
prvall.referring_prv_name_first as "referring_prv_name_first",
prvall.referring_prv_name_middle as "referring_prv_name_middle",
prvall.referring_prv_name_suffix as "referring_prv_name_suffix",
prvall.referring_prv_id as "referring_prv_id",
prvall.referring_prv_ref_id_qlfy as "referring_prv_ref_id_qlfy",
prvall.referring_prv_second_id as "referring_prv_second_id",
prvall.rendering_prv_type_qlfr as "rendering_prv_type_qlfr",
prvall.rendering_prv_name_last as "rendering_prv_name_last",
prvall.rendering_prv_name_first as "rendering_prv_name_first",
prvall.rendering_prv_name_middle as "rendering_prv_name_middle",
prvall.rendering_prv_name_suffix as "rendering_prv_name_suffix",
prvall.rendering_prv_id as "rendering_prv_id",
prvall.rendering_prv_ref_id_qlfy as "rendering_prv_ref_id_qlfy",
prvall.rendering_prv_second_id as "rendering_prv_second_id",
prvall.rendering_speciality_id_qlfr as "rendering_speciality_id_qlfr",
prvall.rendering_prv_speciality_tax_code as "rendering_prv_speciality_tax_code",
prvall.rendering_other_payer_ren_prv_type as "rendering_other_payer_ren_prv_type",
amblnc.clm_amblnc_pickup_addr_1 as "clm_amblnc_pickup_addr_1",
amblnc.clm_amblnc_pickup_addr_2 as "clm_amblnc_pickup_addr_2",
amblnc.clm_amblnc_pickup_city as "clm_amblnc_pickup_city",
amblnc.clm_amblnc_pickup_state as "clm_amblnc_pickup_state",
amblnc.clm_amblnc_pickup_zip as "clm_amblnc_pickup_zip",
amblnc.clm_amblnc_dropoff_location as "clm_amblnc_dropoff_location",
amblnc.clm_amblnc_dropoff_addr_1 as "clm_amblnc_dropoff_addr_1",
amblnc.clm_amblnc_dropoff_addr_2 as "clm_amblnc_dropoff_addr_2",
amblnc.clm_amblnc_dropoff_city as "clm_amblnc_dropoff_city",
amblnc.clm_amblnc_dropoff_state as "clm_amblnc_dropoff_state",
amblnc.clm_amblnc_dropoff_zip as "clm_amblnc_dropoff_zip",
prv.refer_id_qlfy as "refer_id_qlfy",
prv.provider_address_1 as "provider_address_1",
prv.provider_address_2 as "provider_address_2",
prv.provider_city as "provider_city",
prv.provider_contact_name as "provider_contact_name",
prv.provider_contact_type as "provider_contact_type",
prv.provider_contact_no as "provider_contact_no",
clm.non_covered_charge_amt as "non_covered_charge_amt",
clm.sv_reimbursement_rate as "sv_reimbursement_rate",
clm.sv_hcpcs_payable_amt as "sv_hcpcs_payable_amt",
clm.sv_clm_payment_remark_code as "sv_clm_payment_remark_code",
mem.other_subscriber_name_last as "other_subscriber_name_last",
mem.other_subscriber_name_first as "other_subscriber_name_first",
mem.other_subscriber_name_middle as "other_subscriber_name_middle",
mem.other_subscriber_suffix as "other_subscriber_suffix",
mem.other_subscriber_id_qlfr as "other_subscriber_id_qlfr",
mem.other_subscriber_id as "other_subscriber_id",
mem.other_subscriber_address1 as "other_subscriber_address1",
mem.other_subscriber_address2 as "other_subscriber_address2",
mem.other_subscriber_city as "other_subscriber_city",
mem.other_subscriber_state as "other_subscriber_state",
mem.other_subscriber_zip as "other_subscriber_zip"






FROM IDENTIFIER (:V_TMP_CLAIM) clm 
LEFT OUTER JOIN IDENTIFIER (:V_PROF_PROVIDER) prv ON (prv.grp_control_no = clm.grp_control_no
                                                 AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND prv.provider_hl_no = clm.provider_hl_no
                                                 AND prv.transactset_create_date = clm.transactset_create_date
                                                 AND prv.xml_md5 = clm.xml_md5)
LEFT OUTER JOIN IDENTIFIER (:V_PROF_SUBSCRIBER) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date
                                                 AND mem.xml_md5 = clm.xml_md5)
LEFT OUTER JOIN IDENTIFIER (:V_PROF_PATIENT) pat ON (pat.grp_control_no = clm.grp_control_no
                                                 AND pat.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND pat.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND pat.transactset_create_date = clm.transactset_create_date
                                                 AND pat.xml_md5 = clm.xml_md5)
JOIN IDENTIFIER (:V_TMP_DB2IMPORT_CLM_FILTERED_NEW) db2 
ON (db2.member_id = mem.subscriber_id
AND db2.medicare_clm_cntrl_num IS NOT NULL
AND db2.medicare_clm_cntrl_num = clm.payer_clm_ctrl_num
AND clm.vendor_cd=''CMS'')
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.provider_hl_no,
b.subscriber_hl_no,
b.payer_hl_no	,
b.claim_id	,
b.transactset_create_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.current_illness_injury_date)),'''')::string,3,8) as current_illness_injury_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.initial_treatment_date)),'''')::string,3,8) as initial_treatment_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.treatment_therapy_date)),'''')::string,3,8) as treatment_therapy_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.acute_manifestation_date)),'''')::string,3,8) as acute_manifestation_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.accident_date)),'''')::string,3,8) as accident_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.hospitalized_admission_date)),'''')::string,3,8) as hospitalized_admission_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.hospitalized_discharge_date)),'''')::string,3,8) as hospitalized_discharge_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.transactset_create_date,

        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''431''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as current_illness_injury_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''454''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as initial_treatment_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''304''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as treatment_therapy_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''453''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as acute_manifestation_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''439''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as accident_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''435''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as hospitalized_admission_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''096''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as hospitalized_discharge_date

//ARRAY_AGG(a.group_map[''431'']) as current_illness_injury_date,
//ARRAY_AGG(a.group_map[''454'']) as initial_treatment_date,
//ARRAY_AGG(a.group_map[''304'']) as treatment_therapy_date,
//ARRAY_AGG(a.group_map[''453'']) as acute_manifestation_date,
//ARRAY_AGG(a.group_map[''439'']) as accident_date,
//ARRAY_AGG(a.group_map[''435'']) as hospitalized_admission_date,
//ARRAY_AGG(a.group_map[''096'']) as hospitalized_discharge_date	


FROM (SELECT distinct grp_control_no,
trancactset_cntl_no,
provider_hl_no,
subscriber_hl_no,
payer_hl_no,
claim_id,
transactset_create_date,

//OBJECT_INSERT({}, clm_date_type,clm_date) as group_map

OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map

FROM IDENTIFIER (:V_PROF_CLM_SV_DATES) 
where sl_seq_num is null 
and TO_NUMBER(transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
AND dates.transactset_create_date = clm.transactset_create_date)

LEFT OUTER JOIN IDENTIFIER (:V_TMP_AMBLNC) amblnc ON (amblnc.grp_control_no = clm.grp_control_no
AND concat(amblnc.trancactset_cntl_no,''$'',amblnc.provider_hl_no,''$'',amblnc.subscriber_hl_no,''$'',amblnc.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
AND amblnc.transactset_create_date = clm.transactset_create_date)

LEFT OUTER JOIN IDENTIFIER (:V_TMP_PRVALL) prvall ON (clm.grp_control_no = prvall.grp_control_no
AND concat(prvall.trancactset_cntl_no,''$'',prvall.provider_hl_no,''$'',prvall.subscriber_hl_no,''$'',prvall.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
AND clm.transactset_create_date = prvall.transactset_create_date)
--WHERE db2.clh_trk_id = ''F8''

UNION ALL

SELECT DISTINCT
db2.clm_num as "ucps_clm_num",
db2.clm_recept_dt as "exctract_key",
prv.provider_type as "billing_provider_type",
prv.provider_name as "billing_provider_name",
prv.provider_name_first as "billing_provider_name_first",
prv.provider_name_middle as "billing_provider_name_middle",
prv.name_prefix as "billing_provider_name_prefix",
prv.name_suffix as "billing_provider_name_suffix",
prv.provider_id as "billing_provider_id",
prv.provider_tax_code as "billing_provider_tax_code",
prv.payto_ent_type_qlfy as "payto_ent_type_qlfy",
prv.payto_address_1 as "payto_address_1",
prv.payto_address_2 as "payto_address_2",
prv.payto_city as "payto_city",
prv.payto_state as "payto_state",
prv.payto_zip as "payto_zip",
pat.pat_type_qlfr as "patient_type_qlfr",
pat.pat_name_last as "patient_name_last",
pat.pat_name_first as "patient_name_first",
pat.pat_name_middle as "patient_name_middle",
pat.pat_name_suffix as "patient_name_suffix",
pat.pat_address_1 as "patient_address_1",
pat.pat_address_2 as "patient_address_2",
pat.pat_city as "patient_city",
pat.pat_state as "patient_state",
pat.pat_zip as "patient_zip",
pat.pat_dateofbirth as "patient_dateofbirth",
pat.pat_gendercode as "patient_gendercode",
pat.pat_death_dt as "patient_death_dt",
pat.pat_weight_measure_unit as "patient_weight_measure_unit",
pat.pat_weight as "patient_weight" ,
pat.pat_pregnancy_indicator as "patient_pregnancy_indicator" ,
pat.pat_insurer_relationship_cd as "patient_insurer_relationship_cd",
pat.pat_dep_death_dt as "patient_dependent_death_dt"  ,
pat.pat_dep_weight_measure_unit as "patient_dependent_weight_measure_unit" ,
pat.pat_dep_weight as "patient_dependent_weight",
pat.pat_dep_pregnancy_indicator as "patient_dependent_pregnancy_indicator",
mem.payer_responsibility_code as "payer_responsibility_code",
mem.subscriber_relationship_code as "subscriber_relationship_code",
mem.subscriber_policy_number as "subscriber_policy_number",
mem.subscriber_plan_name as "subscriber_plan_name",
mem.subscriber_insurance_type as "subscriber_insurance_type",
mem.claim_type as "claim_filing_indicator_code",
mem.subscriber_type as "subscriber_type",
mem.subscriber_name_last as "subscriber_name_last",
mem.subscriber_name_first as "subscriber_name_first",
mem.subscriber_name_middle as "subscriber_name_middle",
mem.name_prefix as "subscriber_name_prefix",
mem.name_suffix as "subscriber_name_suffix",
mem.subscriber_id_qualifier as "subscriber_id_qualifier",
mem.subscriber_id as "subscriber_id",
mem.subscriber_address1 as "subscriber_address1",
mem.subscriber_address2 as "subscriber_address2",
mem.subscriber_city as "subscriber_city",
mem.subscriber_state as "subscriber_state",
mem.subscriber_postalcode as "subscriber_postalcode",
mem.subscriber_dateofbirth as "subscriber_dateofbirth",
mem.subscriber_gendercode as "subscriber_gendercode",
mem.subscriber_suplemental_id_qlfr as "subscriber_suplemental_id_qlfr",
mem.subscriber_sumplemental_id as "subscriber_suplemental_id",
clm.claim_id as "patient_control_number",
clm.total_claim_charge_amt as "total_claim_charge_amt",
SPLIT(clm.healthcareservice_location,'':'')[0]::string as "place_of_service_cd",
SPLIT(clm.healthcareservice_location,'':'')[1]::string as "facility_cd_qlfr",
SPLIT(clm.healthcareservice_location,'':'')[2]::string as "claim_frequency_type_code",
clm.provider_accept_assign_code as "provider_accept_assign_code",
clm.provider_benefit_auth_code as "provider_benefit_auth_code",
clm.provider_patinfo_release_auth_code as "provider_patinfo_release_auth_code",
clm.pat_signature_cd as "pat_signature_cd",
SPLIT(clm.related_cause_code_info,'':'')[0]::string as "related_cause_code",
SPLIT(clm.related_cause_code_info,'':'')[3]::string as "autoaccident_state",
SPLIT(clm.related_cause_code_info,'':'')[4]::string as "autoaccident_country",
clm.special_program_indicator as "special_program_indicator",
clm.delay_reason_code as "delay_reason_code",
clm.patient_amt_paid as "patient_amt_paid",
clm.service_auth_exception_code as "service_auth_exception_code",
clm.clinical_lab_amendment_num as "clinical_lab_amendment_num",
clm.medical_record_number as "medical_record_number",
clm.demonstration_project_id as "demonstration_project_id",
clm.payer_clm_ctrl_num as "payer_clm_ctrl_num",
clm.network_trace_number as "network_trace_number",
CONCAT_WS(''*'',clm.hc_condition_codes) as "healthcare_condition_codes",
clm.health_care_code_info as "healthcare_diagnosis_codes",
CONCAT_WS(''*'',clm.health_care_additional_code_info) as "health_care_additional_codes",
clm.anesthesia_procedure_code as "anesthesia_procedure_code",
clm.vendor_cd as "clm_submit_vendor",
dates.current_illness_injury_date as "current_illness_injury_date",
dates.initial_treatment_date as "initial_treatment_date",
dates.treatment_therapy_date as "treatment_therapy_date",
dates.acute_manifestation_date as "acute_manifestation_date",
dates.accident_date as "accident_date",
dates.hospitalized_admission_date as "hospitalized_admission_date",
dates.hospitalized_discharge_date as "hospitalized_discharge_date",
prvall.supervising_prv_type_qlfr as "supervising_prv_type_qlfr",
prvall.supervising_prv_name_last as "supervising_prv_name_last",
prvall.supervising_prv_name_first as "supervising_prv_name_first",
prvall.supervising_prv_name_middle as "supervising_prv_name_middle",
prvall.supervising_prv_name_suffix as "supervising_prv_name_suffix",
prvall.supervising_prv_id as "supervising_prv_id",
prvall.supervising_prv_ref_id_qlfy as "supervising_prv_ref_id_qlfy",
prvall.supervising_prv_second_id as "supervising_prv_second_id",
prvall.referring_type_qlfr as "referring_type_qlfr",
prvall.referring_prv_name_last as "referring_prv_name_last",
prvall.referring_prv_name_first as "referring_prv_name_first",
prvall.referring_prv_name_middle as "referring_prv_name_middle",
prvall.referring_prv_name_suffix as "referring_prv_name_suffix",
prvall.referring_prv_id as "referring_prv_id",
prvall.referring_prv_ref_id_qlfy as "referring_prv_ref_id_qlfy",
prvall.referring_prv_second_id as "referring_prv_second_id",
prvall.rendering_prv_type_qlfr as "rendering_prv_type_qlfr",
prvall.rendering_prv_name_last as "rendering_prv_name_last",
prvall.rendering_prv_name_first as "rendering_prv_name_first",
prvall.rendering_prv_name_middle as "rendering_prv_name_middle",
prvall.rendering_prv_name_suffix as "rendering_prv_name_suffix",
prvall.rendering_prv_id as "rendering_prv_id",
prvall.rendering_prv_ref_id_qlfy as "rendering_prv_ref_id_qlfy",
prvall.rendering_prv_second_id as "rendering_prv_second_id",
prvall.rendering_speciality_id_qlfr as "rendering_speciality_id_qlfr",
prvall.rendering_prv_speciality_tax_code as "rendering_prv_speciality_tax_code",
prvall.rendering_other_payer_ren_prv_type as "rendering_other_payer_ren_prv_type",
amblnc.clm_amblnc_pickup_addr_1 as "clm_amblnc_pickup_addr_1",
amblnc.clm_amblnc_pickup_addr_2 as "clm_amblnc_pickup_addr_2",
amblnc.clm_amblnc_pickup_city as "clm_amblnc_pickup_city",
amblnc.clm_amblnc_pickup_state as "clm_amblnc_pickup_state",
amblnc.clm_amblnc_pickup_zip as "clm_amblnc_pickup_zip",
amblnc.clm_amblnc_dropoff_location as "clm_amblnc_dropoff_location",
amblnc.clm_amblnc_dropoff_addr_1 as "clm_amblnc_dropoff_addr_1",
amblnc.clm_amblnc_dropoff_addr_2 as "clm_amblnc_dropoff_addr_2",
amblnc.clm_amblnc_dropoff_city as "clm_amblnc_dropoff_city",
amblnc.clm_amblnc_dropoff_state as "clm_amblnc_dropoff_state",
amblnc.clm_amblnc_dropoff_zip as "clm_amblnc_dropoff_zip",
prv.refer_id_qlfy as "refer_id_qlfy",
prv.provider_address_1 as "provider_address_1",
prv.provider_address_2 as "provider_address_2",
prv.provider_city as "provider_city",
prv.provider_contact_name as "provider_contact_name",
prv.provider_contact_type as "provider_contact_type",
prv.provider_contact_no as "provider_contact_no",
clm.non_covered_charge_amt as "non_covered_charge_amt",
clm.sv_reimbursement_rate as "sv_reimbursement_rate",
clm.sv_hcpcs_payable_amt as "sv_hcpcs_payable_amt",
clm.sv_clm_payment_remark_code as "sv_clm_payment_remark_code",
mem.other_subscriber_name_last as "other_subscriber_name_last",
mem.other_subscriber_name_first as "other_subscriber_name_first",
mem.other_subscriber_name_middle as "other_subscriber_name_middle",
mem.other_subscriber_suffix as "other_subscriber_suffix",
mem.other_subscriber_id_qlfr as "other_subscriber_id_qlfr",
mem.other_subscriber_id as "other_subscriber_id",
mem.other_subscriber_address1 as "other_subscriber_address1",
mem.other_subscriber_address2 as "other_subscriber_address2",
mem.other_subscriber_city as "other_subscriber_city",
mem.other_subscriber_state as "other_subscriber_state",
mem.other_subscriber_zip as "other_subscriber_zip"


FROM IDENTIFIER (:V_TMP_CLAIM) clm
LEFT OUTER JOIN IDENTIFIER (:V_PROF_PROVIDER) prv ON (prv.grp_control_no = clm.grp_control_no
                                                 AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND prv.provider_hl_no = clm.provider_hl_no
                                                 AND prv.transactset_create_date = clm.transactset_create_date
                                                 AND prv.xml_md5 = clm.xml_md5)
LEFT OUTER JOIN IDENTIFIER (:V_PROF_SUBSCRIBER) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date
                                                 AND mem.xml_md5 = clm.xml_md5)
LEFT OUTER JOIN IDENTIFIER (:V_PROF_PATIENT) pat ON (pat.grp_control_no = clm.grp_control_no
                                                 AND pat.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND pat.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND pat.transactset_create_date = clm.transactset_create_date
                                                 AND pat.xml_md5 = clm.xml_md5)
JOIN IDENTIFIER (:V_TMP_DB2IMPORT_CLM_FILTERED_NEW) db2 
ON (db2.clh_trk_id = clm.network_trace_number
AND clm.vendor_cd=''CH''
AND db2.clh_trk_id IS NOT NULL)
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.provider_hl_no,
b.subscriber_hl_no,
b.payer_hl_no	,
b.claim_id	,
b.transactset_create_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.current_illness_injury_date)),'''')::string,3,8) as current_illness_injury_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.initial_treatment_date)),'''')::string,3,8) as initial_treatment_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.treatment_therapy_date)),'''')::string,3,8) as treatment_therapy_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.acute_manifestation_date)),'''')::string,3,8) as acute_manifestation_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.accident_date)),'''')::string,3,8) as accident_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.hospitalized_admission_date)),'''')::string,3,8) as hospitalized_admission_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.hospitalized_discharge_date)),'''')::string,3,8) as hospitalized_discharge_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.transactset_create_date,

        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''431''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as current_illness_injury_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''454''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as initial_treatment_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''304''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as treatment_therapy_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''453''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as acute_manifestation_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''439''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as accident_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''435''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as hospitalized_admission_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''096''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as hospitalized_discharge_date
 

//ARRAY_AGG(a.group_map[''431'']) as current_illness_injury_date,
//ARRAY_AGG(a.group_map[''454'']) as initial_treatment_date,
//ARRAY_AGG(a.group_map[''304'']) as treatment_therapy_date,
//ARRAY_AGG(a.group_map[''453'']) as acute_manifestation_date,
//ARRAY_AGG(a.group_map[''439'']) as accident_date,
//ARRAY_AGG(a.group_map[''435'']) as hospitalized_admission_date,
//ARRAY_AGG(a.group_map[''096'']) as hospitalized_discharge_date	



FROM (SELECT distinct grp_control_no,
trancactset_cntl_no,
provider_hl_no,
subscriber_hl_no,
payer_hl_no,
claim_id,
transactset_create_date,

//OBJECT_INSERT({}, clm_date_type,clm_date) as group_map

OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map

FROM IDENTIFIER (:V_PROF_CLM_SV_DATES) 
where sl_seq_num is null 
and TO_NUMBER(transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.transactset_create_date) b) dates 
ON (dates.grp_control_no = clm.grp_control_no
AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
AND dates.transactset_create_date = clm.transactset_create_date)

LEFT OUTER JOIN IDENTIFIER (:V_TMP_AMBLNC) amblnc ON (amblnc.grp_control_no = clm.grp_control_no
AND concat(amblnc.trancactset_cntl_no,''$'',amblnc.provider_hl_no,''$'',amblnc.subscriber_hl_no,''$'',amblnc.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
AND amblnc.transactset_create_date = clm.transactset_create_date)

LEFT OUTER JOIN IDENTIFIER (:V_TMP_PRVALL) prvall ON (clm.grp_control_no = prvall.grp_control_no
AND concat(prvall.trancactset_cntl_no,''$'',prvall.provider_hl_no,''$'',prvall.subscriber_hl_no,''$'',prvall.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
AND clm.transactset_create_date = prvall.transactset_create_date
AND clm.xml_md5 = prvall.xml_md5) 
--WHERE db2.clh_trk_id = ''D9''
) claim;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_FWA_PROFESSIONAL_CLAIM)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
  
  
V_STEP := ''STEP6'';
   
  
V_STEP_NAME := ''Generate FWA_PROF_CLAIM_REPORT''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      



V_STAGE_QUERY1 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_performant/''||''fwa_professional_claim_report_''||(SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYY_MM''))||'' FROM (
            SELECT * FROM TMP_FWA_PROFESSIONAL_CLAIM
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''~''''
               empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''')
               compression = ''''gzip'''' 
              )
HEADER = TRUE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = False''
;


V_STAGE_QUERY2 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_performant/''||''fwa_professional_claim_report_''||(SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYY_MM''))||'' FROM (
            SELECT 
            
            ''''ucps_clm_num'''',
''''exctract_key'''',
''''billing_provider_type'''',
''''billing_provider_name'''',
''''billing_provider_name_first'''',
''''billing_provider_name_middle'''',
''''billing_provider_name_prefix'''',
''''billing_provider_name_suffix'''',
''''billing_provider_id'''',
''''billing_provider_tax_code'''',
''''payto_ent_type_qlfy'''',
''''payto_address_1'''',
''''payto_address_2'''',
''''payto_city'''',
''''payto_state'''',
''''payto_zip'''',
''''patient_type_qlfr'''',
''''patient_name_last'''',
''''patient_name_first'''',
''''patient_name_middle'''',
''''patient_name_suffix'''',
''''patient_address_1'''',
''''patient_address_2'''',
''''patient_city'''',
''''patient_state'''',
''''patient_zip'''',
''''patient_dateofbirth'''',
''''patient_gendercode'''',
''''patient_death_dt'''',
''''patient_weight_measure_unit'''',
''''patient_weight'''',
''''patient_pregnancy_indicator'''',
''''patient_insurer_relationship_cd'''',
''''patient_dependent_death_dt'''',
''''patient_dependent_weight_measure_unit'''',
''''patient_dependent_weight'''',
''''patient_dependent_pregnancy_indicator'''',
''''payer_responsibility_code'''',
''''subscriber_relationship_code'''',
''''subscriber_policy_number'''',
''''subscriber_plan_name'''',
''''subscriber_insurance_type'''',
''''claim_filing_indicator_code'''',
''''subscriber_type'''',
''''subscriber_name_last'''',
''''subscriber_name_first'''',
''''subscriber_name_middle'''',
''''subscriber_name_prefix'''',
''''subscriber_name_suffix'''',
''''subscriber_id_qualifier'''',
''''subscriber_id'''',
''''subscriber_address1'''',
''''subscriber_address2'''',
''''subscriber_city'''',
''''subscriber_state'''',
''''subscriber_postalcode'''',
''''subscriber_dateofbirth'''',
''''subscriber_gendercode'''',
''''subscriber_suplemental_id_qlfr'''',
''''subscriber_suplemental_id'''',
''''patient_control_number'''',
''''total_claim_charge_amt'''',
''''place_of_service_cd'''',
''''facility_cd_qlfr'''',
''''claim_frequency_type_code'''',
''''provider_accept_assign_code'''',
''''provider_benefit_auth_code'''',
''''provider_patinfo_release_auth_code'''',
''''pat_signature_cd'''',
''''related_cause_code'''',
''''autoaccident_state'''',
''''autoaccident_country'''',
''''special_program_indicator'''',
''''delay_reason_code'''',
''''patient_amt_paid'''',
''''service_auth_exception_code'''',
''''clinical_lab_amendment_num'''',
''''medical_record_number'''',
''''demonstration_project_id'''',
''''payer_clm_ctrl_num'''',
''''network_trace_number'''',
''''healthcare_condition_codes'''',
''''healthcare_diagnosis_codes'''',
''''health_care_additional_codes'''',
''''anesthesia_procedure_code'''',
''''clm_submit_vendor'''',
''''current_illness_injury_date'''',
''''initial_treatment_date'''',
''''treatment_therapy_date'''',
''''acute_manifestation_date'''',
''''accident_date'''',
''''hospitalized_admission_date'''',
''''hospitalized_discharge_date'''',
''''supervising_prv_type_qlfr'''',
''''supervising_prv_name_last'''',
''''supervising_prv_name_first'''',
''''supervising_prv_name_middle'''',
''''supervising_prv_name_suffix'''',
''''supervising_prv_id'''',
''''supervising_prv_ref_id_qlfy'''',
''''supervising_prv_second_id'''',
''''referring_type_qlfr'''',
''''referring_prv_name_last'''',
''''referring_prv_name_first'''',
''''referring_prv_name_middle'''',
''''referring_prv_name_suffix'''',
''''referring_prv_id'''',
''''referring_prv_ref_id_qlfy'''',
''''referring_prv_second_id'''',
''''rendering_prv_type_qlfr'''',
''''rendering_prv_name_last'''',
''''rendering_prv_name_first'''',
''''rendering_prv_name_middle'''',
''''rendering_prv_name_suffix'''',
''''rendering_prv_id'''',
''''rendering_prv_ref_id_qlfy'''',
''''rendering_prv_second_id'''',
''''rendering_speciality_id_qlfr'''',
''''rendering_prv_speciality_tax_code'''',
''''rendering_other_payer_ren_prv_type'''',
''''clm_amblnc_pickup_addr_1'''',
''''clm_amblnc_pickup_addr_2'''',
''''clm_amblnc_pickup_city'''',
''''clm_amblnc_pickup_state'''',
''''clm_amblnc_pickup_zip'''',
''''clm_amblnc_dropoff_location'''',
''''clm_amblnc_dropoff_addr_1'''',
''''clm_amblnc_dropoff_addr_2'''',
''''clm_amblnc_dropoff_city'''',
''''clm_amblnc_dropoff_state'''',
''''clm_amblnc_dropoff_zip'''',
''''refer_id_qlfy'''',
''''provider_address_1'''',
''''provider_address_2'''',
''''provider_city'''',
''''provider_contact_name'''',
''''provider_contact_type'''',
''''provider_contact_no'''',
''''non_covered_charge_amt'''',
''''sv_reimbursement_rate'''',
''''sv_hcpcs_payable_amt'''',
''''sv_clm_payment_remark_code'''',
''''other_subscriber_name_last'''',
''''other_subscriber_name_first'''',
''''other_subscriber_name_middle'''',
''''other_subscriber_suffix'''',
''''other_subscriber_id_qlfr'''',
''''other_subscriber_id'''',
''''other_subscriber_address1'''',
''''other_subscriber_address2'''',
''''other_subscriber_city'''',
''''other_subscriber_state'''',
''''other_subscriber_zip''''

               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''~''''
               empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''')
               compression = ''''gzip'''' 
              )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = False''
;



IF (V_ROWS_LOADED > 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY1;  
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

ELSEIF (V_ROWS_LOADED = 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY2;  
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

END IF;   
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);    

EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';