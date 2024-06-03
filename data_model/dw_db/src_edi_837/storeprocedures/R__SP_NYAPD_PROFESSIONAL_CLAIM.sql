USE SCHEMA SRC_EDI_837;
  
CREATE OR REPLACE PROCEDURE SP_NYAPD_PROFESSIONAL_CLAIM("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "LZ_ISDW_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''NYAPD_PLAN'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''NYAPD_PROFESSIONAL_CLAIM'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;

V_PARAM_START_DATE VARCHAR ;
V_PARAM_END_DATE   VARCHAR ;

V_TMP_CLAIM   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_CLAIM'';
V_TMP_NYAPD_CLM_FILTERED   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_NYAPD_CLM_FILTERED'';
V_TMP_PROF_CLM_NYAPD   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PROF_CLM_NYAPD'';
V_TMP_NYAPD_PROFESSIONAL_CLAIM  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_NYAPD_PROFESSIONAL_CLAIM'';



V_NYAPD_CLAIM_STG            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.NYAPD_CLAIM_STG'';
V_PROF_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLAIM_PART'';
V_PROF_PAYER                 VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PAYER'';
V_PROF_PATIENT               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PATIENT'';
V_PROF_SUBSCRIBER            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_SUBSCRIBER'';
V_PROF_CLM_SV_DATES           VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLM_SV_DATES'';
V_CH_VIEW                    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.CH_VIEW'';


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;



V_PARAM_START_DATE := (SELECT to_varchar(DATEADD(day,-10,MIN(UCPS_CLM_DT)),''YYYYMMDD'') FROM   IDENTIFIER(:V_NYAPD_CLAIM_STG));
V_PARAM_END_DATE :=   (SELECT to_varchar(MAX(UCPS_CLM_DT)::DATE,''YYYYMMDD'') FROM   IDENTIFIER(:V_NYAPD_CLAIM_STG) WHERE UCPS_CLM_DT IS NOT NULL);

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
 
V_STEP_NAME := ''Load TMP_CLAIM''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_CLAIM) AS

SELECT DISTINCT
        clmbase.xml_md5,
        clmbase.grp_control_no,
        clmbase.trancactset_cntl_no,
        clmbase.provider_hl_no,
        clmbase.subscriber_hl_no,
        clmbase.payer_hl_no,
        clmbase.claim_id,
        clmbase.provider_patinfo_release_auth_code,
        clmbase.related_cause_code_info,
        clmbase.provider_benefit_auth_code,
        clmbase.pat_signature_cd,
        clmbase.special_program_indicator,
        clmbase.delay_reason_code,
        clmbase.patient_amt_paid,
        clmbase.service_auth_exception_code,
        clmbase.clinical_lab_amendment_num,
        clmbase.medical_record_number,
        clmbase.demonstration_project_id,
        clmbase.payer_clm_ctrl_num,
        clmbase.network_trace_number,
        clmbase.non_covered_charge_amt,
        clmbase.health_care_code_info,
        clmbase.health_care_additional_code_info,
        clmbase.provider_accept_assign_code,
        clmbase.vendor_cd,
        clmbase.transactset_create_date FROM IDENTIFIER(:V_PROF_CLAIM_PART) clmbase WHERE transactset_create_date BETWEEN :V_PARAM_START_DATE AND :V_PARAM_END_DATE

;

V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_CLAIM) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   
                                 
                                 
V_STEP := ''STEP2'';
 
V_STEP_NAME := ''Load TMP_NYAPD_CLM_FILTERED''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());   

CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) AS

SELECT db2.* FROM IDENTIFIER(:V_CH_VIEW) db2 JOIN IDENTIFIER(:V_NYAPD_CLAIM_STG) nyapd on db2.clm_num = nyapd.clm_num;

V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   


V_STEP := ''STEP3'';
 
V_STEP_NAME := ''Load TMP_PROF_CLM_NYAPD''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());   

CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_PROF_CLM_NYAPD) AS

SELECT DISTINCT claim.* from (
        SELECT
        db2.clm_num as ucps_clm_num,
        mem.subscriber_name_middle as subscriber_name_middle,
        mem.subscriber_address2 as subscriber_address2,
        clm.provider_patinfo_release_auth_code as provider_pat_medrec_release_auth_code,
        --payer.payer_prior_auth_num as prior_auth_number,
        regexp_replace(payer.payer_prior_auth_num,''[^\\\\w\\\\s]'')  as prior_auth_number,
        dates.hospitalized_admission_date as admit_Date,
        split(clm.related_cause_code_info,'':'')[0]::string as clm_accident_indicator,
        dates.accident_date as clm_accident_date,
        split(clm.related_cause_code_info,'':'')[0]::string as clm_auto_accident_indicator,
        clm.health_care_additional_code_info[0]::string as health_care_diagnosis_cd_2,
        clm.health_care_additional_code_info[1]::string as health_care_diagnosis_cd_3,
        dates.hospitalized_discharge_date as discharge_Date,
        clm.provider_accept_assign_code as medicare_assign_indicator,
        clm.medical_record_number as medical_record_number,
        mem.other_subscriber_address2 as other_subscriber_address2,
        dates.current_illness_injury_date as current_illness_injury_date,
        split(clm.related_cause_code_info,'':'')[3]::string as clm_accident_state,
        clm.clinical_lab_amendment_num as clinical_lab_amendment_num,
        pat.pat_name_middle	 as patient_name_middle,
        pat.pat_address_2 as patient_address2	,
        clm.patient_amt_paid as clm_copay_amount,
        clm.non_covered_charge_amt as clm_unconvered_amount,
        clm.patient_amt_paid as deductible_amount,
        clm.patient_amt_paid as copay_amount2
        FROM IDENTIFIER(:V_TMP_CLAIM) clm
        LEFT OUTER JOIN IDENTIFIER(:V_PROF_SUBSCRIBER)  mem ON (mem.grp_control_no = clm.grp_control_no
                                                         AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                         AND mem.transactset_create_date = clm.transactset_create_date
                                                         AND mem.xml_md5 = clm.xml_md5)
        LEFT OUTER JOIN IDENTIFIER(:V_PROF_PATIENT) pat ON (pat.grp_control_no = clm.grp_control_no
                                                         AND pat.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND pat.subscriber_hl_no = clm.subscriber_hl_no
                                                         AND pat.transactset_create_date = clm.transactset_create_date
                                                         AND pat.xml_md5 = clm.xml_md5)
        JOIN IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) db2 ON (db2.member_id = mem.subscriber_id
                                                        AND db2.medicare_clm_cntrl_num IS NOT NULL
                                                        AND db2.medicare_clm_cntrl_num = clm.payer_clm_ctrl_num
        -- Added Date qualifier
        AND MONTH(TO_TIMESTAMP(db2.clm_recept_dt)) = MONTH(TO_TIMESTAMP(clm.transactset_create_date,''YYYYMMDD'') )
        AND YEAR(TO_TIMESTAMP(db2.clm_recept_dt)) =  YEAR(TO_TIMESTAMP(clm.transactset_create_date,''YYYYMMDD'') ) 


)
       


        

        
        LEFT OUTER JOIN IDENTIFIER(:V_PROF_PAYER) payer ON (payer.grp_control_no = clm.grp_control_no
                                                         AND payer.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND payer.payer_hl_no = clm.payer_hl_no
                                                         AND payer.transactset_create_date = clm.transactset_create_date
                                                         AND payer.xml_md5=clm.xml_md5)
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
        
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''431''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as current_illness_injury_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''454''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as initial_treatment_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''304''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as treatment_therapy_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''453''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as acute_manifestation_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''439''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as accident_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''435''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as hospitalized_admission_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''096''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as hospitalized_discharge_date
        

        
        
        FROM (
        
        SELECT distinct grp_control_no,
        trancactset_cntl_no,
        provider_hl_no,
        subscriber_hl_no,
        payer_hl_no,
        claim_id,
        transactset_create_date,
        OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map
        FROM IDENTIFIER(:V_PROF_CLM_SV_DATES) where sl_seq_num is null and transactset_create_date BETWEEN :V_PARAM_START_DATE AND :V_PARAM_END_DATE
        
        
		) a
        group by a.grp_control_no,
        a.trancactset_cntl_no,
        a.provider_hl_no,
        a.subscriber_hl_no,
        a.payer_hl_no,
        a.claim_id,
        a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                                       AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
                                                       AND dates.transactset_create_date = clm.transactset_create_date)
        --Nerwork_Trace_number join
        UNION
        SELECT
        db2.clm_num as ucps_clm_num,
        mem.subscriber_name_middle as subscriber_name_middle,
        mem.subscriber_address2 as subscriber_address2,
        clm.provider_patinfo_release_auth_code as provider_pat_medrec_release_auth_code,
        --payer.payer_prior_auth_num as prior_auth_number,
        regexp_replace(payer.payer_prior_auth_num,''[^\\\\w\\\\s]'')  as prior_auth_number,
        dates.hospitalized_admission_date as admit_Date,
        split(clm.related_cause_code_info,'':'')[0]::string as clm_accident_indicator,
        dates.accident_date as clm_accident_date,
        split(clm.related_cause_code_info,'':'')[0]::string as clm_auto_accident_indicator,
        clm.health_care_additional_code_info[0]::string as health_care_diagnosis_cd_2,
        clm.health_care_additional_code_info[1]::string as health_care_diagnosis_cd_3,
        dates.hospitalized_discharge_date as discharge_Date,
        clm.provider_accept_assign_code as medicare_assign_indicator,
        clm.medical_record_number as medical_record_number,
        mem.other_subscriber_address2 as other_subscriber_address2,
        dates.current_illness_injury_date as current_illness_injury_date,
        split(clm.related_cause_code_info,'':'')[3]::string as clm_accident_state,
        clm.clinical_lab_amendment_num as clinical_lab_amendment_num,
        pat.pat_name_middle	 as patient_name_middle,
        pat.pat_address_2 as patient_address2	,
        clm.patient_amt_paid as clm_copay_amount,
        clm.non_covered_charge_amt as clm_unconvered_amount,
        clm.patient_amt_paid as deductible_amount,
        clm.patient_amt_paid as copay_amount2
        FROM IDENTIFIER(:V_TMP_CLAIM)  clm
        LEFT OUTER JOIN IDENTIFIER(:V_PROF_SUBSCRIBER) mem ON (mem.grp_control_no = clm.grp_control_no
                                                         AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                         AND mem.transactset_create_date = clm.transactset_create_date
                                                         AND mem.xml_md5 = clm.xml_md5)
        LEFT OUTER JOIN IDENTIFIER(:V_PROF_PATIENT) pat ON (pat.grp_control_no = clm.grp_control_no
                                                         AND pat.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND pat.subscriber_hl_no = clm.subscriber_hl_no
                                                         AND pat.transactset_create_date = clm.transactset_create_date
                                                         AND pat.xml_md5 = clm.xml_md5)
        JOIN IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) db2 ON (db2.member_id = mem.subscriber_id
                                                        AND db2.clh_trk_id IS NOT NULL
                                                        AND db2.clh_trk_id = clm.network_trace_number
        )
        LEFT OUTER JOIN IDENTIFIER(:V_PROF_PAYER) payer ON (payer.grp_control_no = clm.grp_control_no
                                                         AND payer.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND payer.payer_hl_no = clm.payer_hl_no
                                                         AND payer.transactset_create_date = clm.transactset_create_date
                                                         AND payer.xml_md5 = clm.xml_md5)
        LEFT OUTER JOIN (SELECT b.grp_control_no,
        b.trancactset_cntl_no,
        b.provider_hl_no,
        b.subscriber_hl_no,
        b.payer_hl_no,
        b.claim_id,
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
        
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''431''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as current_illness_injury_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''454''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as initial_treatment_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''304''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as treatment_therapy_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''453''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as acute_manifestation_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''439''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as accident_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''435''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as hospitalized_admission_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''096''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as hospitalized_discharge_date
        

        
        
        
        
        FROM (SELECT distinct grp_control_no,
        trancactset_cntl_no,
        provider_hl_no,
        subscriber_hl_no,
        payer_hl_no,
        claim_id,
        transactset_create_date,
        OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map
        FROM IDENTIFIER(:V_PROF_CLM_SV_DATES) where sl_seq_num is null and transactset_create_date BETWEEN :V_PARAM_START_DATE AND :V_PARAM_END_DATE
		) a
        group by a.grp_control_no,
        a.trancactset_cntl_no,
        a.provider_hl_no,
        a.subscriber_hl_no,
        a.payer_hl_no,
        a.claim_id,
        a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                                       AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
                                                       AND dates.transactset_create_date = clm.transactset_create_date)) claim

;


V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_PROF_CLM_NYAPD) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   

V_STEP := ''STEP4'';
 
V_STEP_NAME := ''Load TMP_NYAPD_PROFESSIONAL_CLAIM''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());  



ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE; 

CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_NYAPD_PROFESSIONAL_CLAIM) AS 

        select ucps_clm_num as "ucps_clm_num",
        subscriber_name_middle as "subscriber_name_middle",
        subscriber_address2 as "subscriber_address2",
        provider_pat_medrec_release_auth_code as "provider_pat_medrec_release_auth_code",
        prior_auth_number as "prior_auth_number",
        admit_Date as "admit_Date",
        clm_accident_indicator as "clm_accident_indicator",
        clm_accident_date as "clm_accident_date",
        clm_auto_accident_indicator as "clm_auto_accident_indicator",
        health_care_diagnosis_cd_2 as "health_care_diagnosis_cd_2",
        health_care_diagnosis_cd_3 as "health_care_diagnosis_cd_3",
        discharge_Date as "discharge_Date",
        medicare_assign_indicator as "medicare_assign_indicator",
        medical_record_number as "medical_record_number",
        other_subscriber_address2 as "other_subscriber_address2",
        current_illness_injury_date as "current_illness_injury_date",
        clm_accident_state as "clm_accident_state",
        clinical_lab_amendment_num as "clinical_lab_amendment_num",
        patient_name_middle as "patient_name_middle",
        patient_address2 as "patient_address2",
        clm_copay_amount as "clm_copay_amount",
        clm_unconvered_amount as "clm_unconvered_amount",
        deductible_amount as "deductible_amount",
        copay_amount2 as "copay_amount" from (
        select m.*,
        ROW_NUMBER() OVER (PARTITION BY ucps_clm_num ORDER BY subscriber_name_middle DESC NULLS LAST,subscriber_address2 DESC NULLS LAST,prior_auth_number DESC NULLS LAST,health_care_diagnosis_cd_2 DESC NULLS LAST,health_care_diagnosis_cd_3 DESC NULLS LAST,
        other_subscriber_address2 DESC NULLS LAST,clm_copay_amount DESC NULLS LAST,deductible_amount DESC NULLS LAST,copay_amount2 DESC NULLS LAST) AS RN
        from IDENTIFIER(:V_TMP_PROF_CLM_NYAPD) m) as m2 WHERE m2.RN = 1;


V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_NYAPD_PROFESSIONAL_CLAIM) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);      




V_STEP := ''STEP5'';
   
 
V_STEP_NAME := ''Generate NYAPD_PROF_CLAIM_REPORT''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      


V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/nyapd_plan/''||(SELECT CURRENT_DATE)||''/nyapd_prof_claim_report''||'' FROM (
             SELECT *
               FROM TMP_NYAPD_PROFESSIONAL_CLAIM
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''*''''
			   empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''')
               compression = None 
              )
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;



execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);



                              
                                 

EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
