USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_FWA_INSTITUTIONAL_CLAIM("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '

DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME             VARCHAR        default ''FWA_PERFORMANT'';
V_SUB_PROCESS_NAME         VARCHAR        default ''FWA_INSTITUTIONAL_CLAIM'';
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

    


V_TMP_FWA_INSTITUTIONAL_CLAIM               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_FWA_INSTITUTIONAL_CLAIM'';

V_TMP_DB2IMPORT_CLM_FILTERED_NEW          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_DB2IMPORT_CLM_FILTERED_NEW'';
    
V_INST_CLAIM_PART          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_claim_part'';
V_INST_PROVIDER_ALL        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_provider_all'';
V_INST_CLM_SV_DATES        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_clm_sv_dates'';
V_INST_SUBSCRIBER          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_subscriber'';
V_INST_PROVIDER            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_provider'';
V_INST_PATIENT             VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_patient'';
V_INST_PAYER               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_payer'';
V_CH_VIEW                  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.ch_view'';



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
   
V_STEP_NAME := ''Load TMP_FWA_INSTITUTIONAL_CLAIM'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE; 

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_FWA_INSTITUTIONAL_CLAIM) AS

SELECT DISTINCT claim.* FROM (SELECT
db2.clm_num as "ucps_clm_num",
db2.clm_recept_dt as "extract_key",
prv.provider_tax_code as "billing_provider_tax_code",
prv.provider_name as "billing_provider_name",
prv.provider_id as "billing_provider_id",
prv.provider_address_1 as "billing_provider_address_1",
prv.provider_address_2 as "billing_provider_address_2",
prv.provider_city as "billing_provider_city",
prv.provider_state as "billing_provider_state",
prv.provider_postalcode as "billing_provider_postalcode",
prv.provider_tax_id as "billing_provider_tax_id",
prv.provider_contact_name as "billing_provider_contact_name",
prv.provider_contact_no as "billing_provider_contact_no",
prv.payto_ent_type as "billing_payto_ent_type",
prv.payto_orgnization_name as "billing_payto_orgnization_name",
prv.payto_ent_id as "billing_payto_ent_id",
prvall.clm_attending_prv_map[''prv_name_last'']::string as "attending_prv_name_last",
prvall.clm_attending_prv_map[''prv_name_first'']::string as "attending_prv_name_first",
prvall.clm_attending_prv_map[''prv_name_middle'']::string as "attending_prv_name_middle",
prvall.clm_attending_prv_map[''prv_name_suffix'']::string as "attending_prv_name_suffix",
prvall.clm_attending_prv_map[''prv_id'']::string as "attending_prv_id",
prvall.clm_attending_prv_map[''prv_speciality_tax_code'']::string as "attending_prv_speciality_tax_code",
prvall.clm_operating_phys_map[''prv_name_last'']::string as "operating_phys_name_last",
prvall.clm_operating_phys_map[''prv_name_first'']::string as "operating_phys_name_first",
prvall.clm_operating_phys_map[''prv_name_middle'']::string as "operating_phys_name_middle",
prvall.clm_operating_phys_map[''prv_name_suffix'']::string as "operating_phys_name_suffix",
prvall.clm_operating_phys_map[''prv_id'']::string as "operating_phys_id",
prvall.clm_rendering_prv_map[''prv_type_qlfr'']::string as "rendering_prv_type_qlfr",
prvall.clm_rendering_prv_map[''prv_name_last'']::string as "rendering_prv_name_last",
prvall.clm_rendering_prv_map[''prv_name_first'']::string as "rendering_prv_name_first",
prvall.clm_rendering_prv_map[''prv_name_middle'']::string as "rendering_prv_name_middle",
prvall.clm_rendering_prv_map[''prv_name_suffix'']::string as "rendering_prv_name_suffix",
prvall.clm_rendering_prv_map[''prv_id'']::string as "rendering_prv_id",
prvall.clm_referring_prv_map[''prv_name_last'']::string as "referring_prv_name_last",
prvall.clm_referring_prv_map[''prv_name_first'']::string as "referring_prv_name_first",
prvall.clm_referring_prv_map[''prv_name_middle'']::string as "referring_prv_name_middle",
prvall.clm_referring_prv_map[''prv_name_suffix'']::string as "referring_prv_name_suffix",
prvall.clm_referring_prv_map[''prv_id'']::string as "referring_prv_id",
prvall.clm_referring_prv_map[''prv_secondary_id'']::string as "referring_prv_secondary_id",
mem.payer_responsibility_code as "payer_responsibility_code",
mem.subscriber_relationship_code as "subscriber_relationship_code",
mem.subscriber_policy_number as "subscriber_policy_number",
mem.subscriber_plan_name as "subscriber_plan_name",
mem.claim_type as "claim_filing_indicator_code",
mem.subscriber_type as "subscriber_type",
mem.subscriber_name_last as "subscriber_name_last",
mem.subscriber_name_first as "subscriber_name_first",
mem.subscriber_name_middle as "subscriber_name_middle",
mem.name_suffix as "subscriber_name_suffix",
mem.subscriber_id_qualifier as "subscriber_id_qualifier",
mem.subscriber_id as "subscriber_id",
mem.subscriber_address1 as "subscriber_address1",
mem.subscriber_address2 as "subscriber_address2",
mem.subscriber_city as "subscriber_city",
mem.subscriber_state as "subscriber_state",
mem.subscriber_postalcode as "subscriber_postalcode",
mem.other_payer_responsibility_code as "other_payer_responsibility_code",
mem.other_subscriber_relationship_code as "other_subscriber_relationship_code",
mem.other_subscriber_policy_number as "other_subscriber_policy_number",
mem.other_subscriber_plan_name as "other_subscriber_plan_name",
mem.other_clm_filling_ind_cd as "other_subscriber_clm_filling_ind_cd",
mem.other_subscriber_type as "other_subscriber_type",
mem.other_subscriber_name_last as "other_subscriber_name_last",
mem.other_subscriber_name_first as "other_subscriber_name_first",
mem.other_subscriber_name_middle as "other_subscriber_name_middle",
mem.other_subscriber_name_suffix as "other_subscriber_name_suffix",
mem.other_subscriber_id_qlfr as "other_subscriber_id_qualifier",
mem.other_subscriber_id as "other_subscriber_id",
clm.claim_id as "patient_control_number",
clm.total_claim_charge_amt as "total_claim_charge_amt",
SPLIT(clm.healthcareservice_location,'':'')[0]::string as "place_of_service_cd",
SPLIT(clm.healthcareservice_location,'':'')[1]::string as "facility_cd_qlfr",
SPLIT(clm.healthcareservice_location,'':'')[2]::string as "claim_frequency_type_code",
clm.provider_accept_assign_code as "provider_accept_assign_code",
clm.provider_benefit_auth_code as "provider_benefit_auth_code",
clm.provider_patinfo_release_auth_code as "provider_patinfo_release_auth_code",
clm.delay_reason_code as "delay_reason_code",
ARRAY_TO_STRING(clm.hc_condition_codes,'''') as "healthcare_condition_codes",
SPLIT(clm.health_care_code_info,'':'')[0]::string as "principle_diagnosis_cd_qlfr",
SPLIT(clm.health_care_code_info,'':'')[1]::string as "principle_diagnosis_cd",
SPLIT(clm.health_care_code_info,'':'')[8]::string as "present_on_admission_indicator",
clm.clm_lab_facility_addr1 as "clm_lab_facility_addr1",
clm.clm_lab_facility_addr2 as "clm_lab_facility_addr2",
clm.clm_lab_facility_city as "clm_lab_facility_city",
clm.clm_lab_facility_state as "clm_lab_facility_state",
clm.clm_lab_facility_zip as "clm_lab_facility_zip",
clm.clm_lab_facility_name as "clm_lab_facility_name",
clm.clm_lab_facility_id as "clm_lab_facility_id",
clm.clm_lab_facility_ref_id_qlfr as "clm_lab_facility_ref_id_qlfr",
clm.clm_lab_facility_ref_id as "clm_lab_facility_ref_id",
clm.admit_type_code as "admit_type_code",
clm.admit_source_code as "admit_source_code",
clm.patient_status_code as "patient_status_code",
SPLIT(clm.principal_procedure_info,'':'')[0]::string as "principal_procedure_cd_qlfr",
SPLIT(clm.principal_procedure_info,'':'')[1]::string as "principal_procedure_code",
SPLIT(clm.principal_procedure_info,'':'')[2]::string as "principal_procedure_dt_qlfr",
SPLIT(clm.principal_procedure_info,'':'')[3]::string as "principal_procedure_dt",
dates.discharge_datetime as "discharge_datetime",
//SPLIT(dates.statement_dates,''-'')[0]::string as "statement_from_date",
//SPLIT(dates.statement_dates,''-'')[1]::string as "statement_to_date",
dates.statement_from_date as "statement_from_date",
dates.statement_to_date as "statement_to_date",
dates.admission_datetime as "admission_datetime",
dates.repricer_received_date as "repricer_received_date",
payer.payer_name as "payer_name",
payer.identification_cd_qlfy as "payer_id_cd_qlfy",
payer.payer_id as "payer_id",
payer.payer_address_1 as "payer_address_1",
payer.payer_address_2 as "payer_address_2",
payer.payer_city as "payer_city",
payer.payer_state as "payer_state",
payer.payer_postalcode as "payer_postalcode",
payer.other_payer_type as "other_payer_type",
payer.other_payer_name as "other_payer_name",
payer.other_identification_cd_qlfy as "other_payer_id_cd_qlfy",
payer.other_payer_id as "other_payer_id",
pat.pat_insurer_relationship_cd as "pat_insurer_relationship_cd",
pat.pat_type_qlfr as "pat_type_qlfr",
pat.pat_name_last as "pat_name_last",
pat.pat_name_first as "pat_name_first",
pat.pat_name_middle as "pat_name_middle",
pat.pat_name_suffix as "pat_name_suffix",
clm.other_payer_1_paid_amt as "other_payer_1_paid_amt",
clm.other_payer_2_paid_amt as "other_payer_2_paid_amt",
clm.sv_hcpcs_payable_amt as "sv_hcpcs_payable_amt",
clm.cas_adj_amt as "cas_adj_amt",
mem.subscriber_dateofbirth as "subscriber_dateofbirth",
mem.subscriber_gendercode as "subscriber_gendercode",
clm.medical_record_number as "medical_record_number",
clm.clm_note_text as "clm_note_text",
clm.clm_billing_note_text as "clm_billing_note_text",
clm.clm_admitting_diagnosis_cd as "clm_admitting_diagnosis_cd",
ARRAY_TO_STRING(clm.patient_reason_for_visit_cd,'''') as "patient_reason_for_visit_cd",
ARRAY_TO_STRING(clm.external_cause_of_injury,'''') as "external_cause_of_injury",
clm.diagnosis_related_grp_info as "diagnosis_related_grp_info",
ARRAY_TO_STRING(clm.other_diagnosis_cd_info,'''') as "other_diagnosis_cd_info",
ARRAY_TO_STRING(clm.other_procedure_info,'''') as "other_procedure_info",
ARRAY_TO_STRING(clm.occurrence_span_info,'''') as "occurrence_span_info",
ARRAY_TO_STRING(clm.occurrence_info,'''') as "occurrence_info",
ARRAY_TO_STRING(clm.value_info,'''') as "value_info",
clm.treatment_cd_info as "treatment_cd_info",
payer.other_payer_prior_auth_num as "other_payer_prior_auth_num",
clm.sv_reimbursement_rate as "sv_reimbursement_rate",
clm.sv_clm_payment_remark_code as "sv_clm_payment_remark_code",
mem.other_subscriber_address_1 as "other_subscriber_address_1",
mem.other_subscriber_address_2 as "other_subscriber_address_2",
mem.other_subscriber_city as "other_subscriber_city",
mem.other_subscriber_state as "other_subscriber_state",
mem.other_subscriber_zip as "other_subscriber_zip",
payer.other_payer_2_name as "other_payer_2_name",
payer.other_payer_2_id as "other_payer_2_id",
payer.other_payer_address_1 as "other_payer_address_1",
payer.other_payer_address_2 as "other_payer_address_2",
payer.other_payer_2_address_1 as "other_payer_2_address_1",
payer.other_payer_2_address_2 as "other_payer_2_address_2",
payer.other_payer_city as "other_payer_city",
payer.other_payer_state as "other_payer_state",
payer.other_payer_zip as "other_payer_zip",
payer.other_payer_2_city as "other_payer_2_city",
payer.other_payer_2_state as "other_payer_2_state",
payer.other_payer_2_zip as "other_payer_2_zip",
payer.other_payer_clm_adjust_indicator as "other_payer_clm_adjust_indicator",
payer.other_payer_2_clm_adjust_indicator as "other_payer_2_clm_adjust_indicator",
payer.other_payer_clm_ctrl_num as "other_payer_clm_ctrl_num",
payer.other_payer_2_clm_ctrl_num as "other_payer_2_clm_ctrl_num",
clm.payer_clm_ctrl_num as "payer_clm_ctrl_num",
clm.clm_note_ref_cd as "clm_note_ref_cd"


FROM IDENTIFIER(:V_INST_PROVIDER_ALL) prvall

JOIN IDENTIFIER(:V_INST_CLAIM_PART) clm ON (clm.grp_control_no = prvall.grp_control_no
                                               AND clm.trancactset_cntl_no = prvall.trancactset_cntl_no
                                               AND clm.claim_id = prvall.claim_id
                                               AND clm.sl_seq_num = prvall.sv_lx_number
                                               AND clm.transactset_create_date = prvall.transactset_create_date)
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.claim_id	,
b.transactset_create_date,
//SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.discharge_datetime)),'''')::string,3,8) as discharge_datetime,
//SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.statement_dates)),'''')::string,3,8) as statement_dates,
//SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.admission_datetime)),'''')::string,3,8) as admission_datetime,
//SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.repricer_received_date)),'''')::string,3,8) as repricer_received_date
split(b.statement_dates,''-'')[0]::STRING as statement_from_date,
split(b.statement_dates,''-'')[1]::STRING  as statement_to_date,
b.discharge_datetime,
b.admission_datetime,
b.repricer_received_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.claim_id,
a.transactset_create_date,
       
      // COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''096''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as discharge_datetime,
      // COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''434''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as statement_dates,
      // COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''435''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as admission_datetime,
      // COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''050''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as repricer_received_date
  
  
        LISTAGG(a.group_map[''096''],''-'') discharge_datetime,
        LISTAGG(a.group_map[''434''],''-'') statement_dates,
        LISTAGG(a.group_map[''435''],''-'') admission_datetime,
        LISTAGG(a.group_map[''050''],''-'') repricer_received_date


//ARRAY_AGG(a.group_map[''096'']) as discharge_datetime,
//ARRAY_AGG(a.group_map[''434'']) as statement_dates,
//ARRAY_AGG(a.group_map[''435'']) as admission_datetime,
//ARRAY_AGG(a.group_map[''050'']) as repricer_received_date		


FROM (SELECT grp_control_no,
trancactset_cntl_no,
claim_id,
transactset_create_date,


//OBJECT_INSERT({}, clm_date_type,clm_date) as group_map

//OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map
OBJECT_CONSTRUCT(clm_date_type,clm_date) as group_map


FROM IDENTIFIER (:V_INST_CLM_SV_DATES) where sl_seq_num is null) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.claim_id,
a.transactset_create_date) b) dates ON (dates.grp_control_no = prvall.grp_control_no
                                               AND dates.trancactset_cntl_no = prvall.trancactset_cntl_no
                                               AND dates.claim_id = prvall.claim_id
                                               AND dates.transactset_create_date = prvall.transactset_create_date)
JOIN IDENTIFIER (:V_INST_PROVIDER) prv ON (prv.grp_control_no = clm.grp_control_no
                                                 AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND prv.transactset_create_date = clm.transactset_create_date)
JOIN IDENTIFIER (:V_INST_SUBSCRIBER) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date)
JOIN IDENTIFIER (:V_INST_PATIENT) pat ON (pat.grp_control_no = clm.grp_control_no
                                                 AND pat.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND pat.transactset_create_date = clm.transactset_create_date)
JOIN IDENTIFIER (:V_INST_PAYER) payer ON (payer.grp_control_no = clm.grp_control_no
                                                 AND payer.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND payer.transactset_create_date = clm.transactset_create_date)


JOIN IDENTIFIER (:V_TMP_DB2IMPORT_CLM_FILTERED_NEW) db2 ON (db2.clh_trk_id = clm.network_trace_number
AND YEAR(TO_DATE(db2.clm_recept_dt)) = YEAR(TO_DATE(clm.transactset_create_date, ''yyyymmdd''))
) 
WHERE TO_NUMBER(clm.transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END) and clm.app_sender_code = ''APTIX'') claim;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_FWA_INSTITUTIONAL_CLAIM)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
V_STEP := ''STEP3'';
   
  
V_STEP_NAME := ''Generate FWA_INST_CLAIM_REPORT''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      



V_STAGE_QUERY1 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_performant/''||''fwa_institutional_claim_report_''||(SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYY_MM''))||'' FROM (
             
             (SELECT *
               FROM TMP_FWA_INSTITUTIONAL_CLAIM)
               
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


V_STAGE_QUERY2 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_performant/''||''fwa_institutional_claim_report_''||(SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYY_MM''))||'' FROM (
             
             (SELECT ''''ucps_clm_num'''',
''''extract_key'''',
''''billing_provider_tax_code'''',
''''billing_provider_name'''',
''''billing_provider_id'''',
''''billing_provider_address_1'''',
''''billing_provider_address_2'''',
''''billing_provider_city'''',
''''billing_provider_state'''',
''''billing_provider_postalcode'''',
''''billing_provider_tax_id'''',
''''billing_provider_contact_name'''',
''''billing_provider_contact_no'''',
''''billing_payto_ent_type'''',
''''billing_payto_orgnization_name'''',
''''billing_payto_ent_id'''',
''''attending_prv_name_last'''',
''''attending_prv_name_first'''',
''''attending_prv_name_middle'''',
''''attending_prv_name_suffix'''',
''''attending_prv_id'''',
''''attending_prv_speciality_tax_code'''',
''''operating_phys_name_last'''',
''''operating_phys_name_first'''',
''''operating_phys_name_middle'''',
''''operating_phys_name_suffix'''',
''''operating_phys_id'''',
''''rendering_prv_type_qlfr'''',
''''rendering_prv_name_last'''',
''''rendering_prv_name_first'''',
''''rendering_prv_name_middle'''',
''''rendering_prv_name_suffix'''',
''''rendering_prv_id'''',
''''referring_prv_name_last'''',
''''referring_prv_name_first'''',
''''referring_prv_name_middle'''',
''''referring_prv_name_suffix'''',
''''referring_prv_id'''',
''''referring_prv_secondary_id'''',
''''payer_responsibility_code'''',
''''subscriber_relationship_code'''',
''''subscriber_policy_number'''',
''''subscriber_plan_name'''',
''''claim_filing_indicator_code'''',
''''subscriber_type'''',
''''subscriber_name_last'''',
''''subscriber_name_first'''',
''''subscriber_name_middle'''',
''''subscriber_name_suffix'''',
''''subscriber_id_qualifier'''',
''''subscriber_id'''',
''''subscriber_address1'''',
''''subscriber_address2'''',
''''subscriber_city'''',
''''subscriber_state'''',
''''subscriber_postalcode'''',
''''other_payer_responsibility_code'''',
''''other_subscriber_relationship_code'''',
''''other_subscriber_policy_number'''',
''''other_subscriber_plan_name'''',
''''other_subscriber_clm_filling_ind_cd'''',
''''other_subscriber_type'''',
''''other_subscriber_name_last'''',
''''other_subscriber_name_first'''',
''''other_subscriber_name_middle'''',
''''other_subscriber_name_suffix'''',
''''other_subscriber_id_qualifier'''',
''''other_subscriber_id'''',
''''patient_control_number'''',
''''total_claim_charge_amt'''',
''''place_of_service_cd'''',
''''facility_cd_qlfr'''',
''''claim_frequency_type_code'''',
''''provider_accept_assign_code'''',
''''provider_benefit_auth_code'''',
''''provider_patinfo_release_auth_code'''',
''''delay_reason_code'''',
''''healthcare_condition_codes'''',
''''principle_diagnosis_cd_qlfr'''',
''''principle_diagnosis_cd'''',
''''present_on_admission_indicator'''',
''''clm_lab_facility_addr1'''',
''''clm_lab_facility_addr2'''',
''''clm_lab_facility_city'''',
''''clm_lab_facility_state'''',
''''clm_lab_facility_zip'''',
''''clm_lab_facility_name'''',
''''clm_lab_facility_id'''',
''''clm_lab_facility_ref_id_qlfr'''',
''''clm_lab_facility_ref_id'''',
''''admit_type_code'''',
''''admit_source_code'''',
''''patient_status_code'''',
''''principal_procedure_cd_qlfr'''',
''''principal_procedure_code'''',
''''principal_procedure_dt_qlfr'''',
''''principal_procedure_dt'''',
''''discharge_datetime'''',
''''statement_from_date'''',
''''statement_to_date'''',
''''admission_datetime'''',
''''repricer_received_date'''',
''''payer_name'''',
''''payer_id_cd_qlfy'''',
''''payer_id'''',
''''payer_address_1'''',
''''payer_address_2'''',
''''payer_city'''',
''''payer_state'''',
''''payer_postalcode'''',
''''other_payer_type'''',
''''other_payer_name'''',
''''other_payer_id_cd_qlfy'''',
''''other_payer_id'''',
''''pat_insurer_relationship_cd'''',
''''pat_type_qlfr'''',
''''pat_name_last'''',
''''pat_name_first'''',
''''pat_name_middle'''',
''''pat_name_suffix'''',
''''other_payer_1_paid_amt'''',
''''other_payer_2_paid_amt'''',
''''sv_hcpcs_payable_amt'''',
''''cas_adj_amt'''',
''''subscriber_dateofbirth'''',
''''subscriber_gendercode'''',
''''medical_record_number'''',
''''clm_note_text'''',
''''clm_billing_note_text'''',
''''clm_admitting_diagnosis_cd'''',
''''patient_reason_for_visit_cd'''',
''''external_cause_of_injury'''',
''''diagnosis_related_grp_info'''',
''''other_diagnosis_cd_info'''',
''''other_procedure_info'''',
''''occurrence_span_info'''',
''''occurrence_info'''',
''''value_info'''',
''''treatment_cd_info'''',
''''other_payer_prior_auth_num'''',
''''sv_reimbursement_rate'''',
''''sv_clm_payment_remark_code'''',
''''other_subscriber_address_1'''',
''''other_subscriber_address_2'''',
''''other_subscriber_city'''',
''''other_subscriber_state'''',
''''other_subscriber_zip'''',
''''other_payer_2_name'''',
''''other_payer_2_id'''',
''''other_payer_address_1'''',
''''other_payer_address_2'''',
''''other_payer_2_address_1'''',
''''other_payer_2_address_2'''',
''''other_payer_city'''',
''''other_payer_state'''',
''''other_payer_zip'''',
''''other_payer_2_city'''',
''''other_payer_2_state'''',
''''other_payer_2_zip'''',
''''other_payer_clm_adjust_indicator'''',
''''other_payer_2_clm_adjust_indicator'''',
''''other_payer_clm_ctrl_num'''',
''''other_payer_2_clm_ctrl_num'''',
''''payer_clm_ctrl_num'''',
''''clm_note_ref_cd''''
)
               
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