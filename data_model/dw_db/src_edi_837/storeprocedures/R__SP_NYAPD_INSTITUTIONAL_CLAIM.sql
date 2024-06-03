USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_NYAPD_INSTITUTIONAL_CLAIM("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "LZ_ISDW_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
 
V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''NYAPD_PLAN'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''NYAPD_INSTITUTIONAL_CLAIM'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY1     VARCHAR;
V_STAGE_QUERY2     VARCHAR;

V_PARAM_START_DATE VARCHAR ;
V_PARAM_END_DATE   VARCHAR ;


V_TMP_NYAPD_CLM_FILTERED   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_NYAPD_CLM_FILTERED'';

V_TMP_NYAPD_INSTITUTIONAL_CLAIM  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_NYAPD_INSTITUTIONAL_CLAIM'';



V_NYAPD_CLAIM_STG            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.NYAPD_CLAIM_STG'';
V_INST_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLAIM_PART'';
V_INST_PAYER                 VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_PAYER'';
V_INST_PATIENT               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_PATIENT'';
V_INST_SUBSCRIBER            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_SUBSCRIBER'';
V_INST_CLM_SV_DATES           VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLM_SV_DATES'';
V_CH_VIEW                    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.CH_VIEW'';


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

V_PARAM_START_DATE := (SELECT to_varchar(DATEADD(day,-10,MIN(UCPS_CLM_DT)),''YYYYMMDD'') FROM   IDENTIFIER(:V_NYAPD_CLAIM_STG));
V_PARAM_END_DATE :=   (SELECT to_varchar(MAX(UCPS_CLM_DT)::DATE,''YYYYMMDD'') FROM   IDENTIFIER(:V_NYAPD_CLAIM_STG) WHERE UCPS_CLM_DT IS NOT NULL);



ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
 
V_STEP_NAME := ''Load TMP_NYAPD_CLM_FILTERED''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) AS

SELECT db2.* FROM IDENTIFIER(:V_CH_VIEW) db2 JOIN IDENTIFIER(:V_NYAPD_CLAIM_STG) nyapd on db2.clm_num = nyapd.clm_num;

V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);    


V_STEP := ''STEP2'';
 
V_STEP_NAME := ''Load TMP_NYAPD_INSTITUTIONAL_CLAIM''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_NYAPD_INSTITUTIONAL_CLAIM) AS 

       SELECT DISTINCT claim.* from (SELECT
        db2.clm_num as "ucps_clm_num",
        clm.clm_billing_note_text as "category_of_service",
        clm.healthcareservice_location as "place_of_service",
        clm.medical_record_number as "medical_record_number",
        substr(dates.hospitalized_admission_date,3,8) as "admission_Date",
        substr(dates.hospitalized_admission_date,11,4) as "admission_hours",
        clm.admit_source_code as "admission_source",
        clm.admit_type_code as "admit_type",
        clm.clm_admitting_diagnosis_cd as "admit_diagnosis",
        substr(dates.hospitalized_discharge_date,1,4) as "discharge_time",
        clm.patient_status_code as "patient_status_code",
        mem.subscriber_name_middle as "subscriber_name_middle",
        mem.subscriber_address2 as "subscriber_address2",
        clm.provider_patinfo_release_auth_code as "medical_info_release_ind",
        clm.provider_accept_assign_code as "medicare_assign_indicator",
        clm.diagnosis_related_grp_info as "diagnosis_relating_grp_code",
        split(clm.other_diagnosis_cd_info[1],'':'')[1]::string as "diagnosis_code_2",
        split(clm.other_diagnosis_cd_info[2],'':'')[1]::string as "diagnosis_code_3",
        split(clm.other_procedure_info[0],'':'')[1]::string as "procedure_code_1",
        split(clm.other_procedure_info[0],'':'')[3]::string as "procedure_date_1",
        split(clm.other_procedure_info[1],'':'')[1]::string as "procedure_code_2",
        split(clm.other_procedure_info[1],'':'')[3]::string as "procedure_date_2",
        split(clm.other_procedure_info[2],'':'')[1]::string as "procedure_code_3",
        split(clm.other_procedure_info[2],'':'')[3]::string as "procedure_date_3",
        split(clm.occurrence_span_info[0],'':'')[1]::string as "Occurrence_span_code",
        split(split(clm.occurrence_span_info[0],'':'')[3],''-'')[0]::string as "occurrence_span_from",
        split(split(clm.occurrence_span_info[0],'':'')[3],''-'')[1]::string as "occurrence_span_to",
        split(clm.occurrence_span_info[1],'':'')[1]::string as "occurrence_span_code_2",
        split(split(clm.occurrence_span_info[1],'':'')[3],''-'')[0]::string as "occurrence_span_from_2",
        split(split(clm.occurrence_span_info[1],'':'')[3],''-'')[1]::string as "occurrence_span_to_2",
        '''' as "occurrence_span_code_1",
        split(clm.occurrence_span_info[2],'':'')[1]::string as "occurrence_span_code_3",
        split(clm.occurrence_info[0],'':'')[1]::string as "occurrence_code_1",
        split(clm.occurrence_info[0],'':'')[3]::string as "occurrence_date_1",
        split(clm.occurrence_info[1],'':'')[1]::string as "occurrence_code_2",
        split(clm.occurrence_info[1],'':'')[3]::string as "occurrence_date_2",
        split(clm.occurrence_info[2],'':'')[1]::string as "occurrence_code_3",
        split(clm.occurrence_info[2],'':'')[3]::string as "occurrence_date_3",
        split(clm.hc_condition_codes[0],'':'')[1]::string as "condition_code_1",
        split(clm.hc_condition_codes[1],'':'')[1]::string as "condition_code_2",
        split(clm.hc_condition_codes[2],'':'')[1]::string as "condition_code_3",
        NVL(NULLIF(split(clm.value_info[0],'':'')[4]::string,''''),split(clm.value_info[0],'':'')[5]::string) as "value_1_amount",
        split(clm.value_info[0],'':'')[1]::string as "value_1_code",
        NVL(NULLIF(split(clm.value_info[1],'':'')[4]::string,''''),split(clm.value_info[1],'':'')[5]::string) as "value_2_amount",
        split(clm.value_info[1],'':'')[1]::string as "value_2_code",
        NVL(NULLIF(split(clm.value_info[2],'':'')[4]::string,''''),split(clm.value_info[2],'':'')[5]::string) as "value_3_amount",  
        split(clm.value_info[2],'':'')[1]::string as "value_3_code",
        split(clm.external_cause_of_injury[0],'':'')[1]::string as "external_cause_code",
        split(clm.health_care_code_info,'':'')[8]::string as "diagnosis_present_on_admit_ind_1",
        split(clm.other_diagnosis_cd_info[0],'':'')[8]::string as "diagnosis_present_on_admit_ind_2",
        split(clm.other_diagnosis_cd_info[1],'':'')[8]::string as "diagnosis_present_on_admit_ind_3",
        -- payer.other_payer_2_id as "other_payer_2_carrier_code",
        -- payer.other_payer_2_name as "other_payer_2_carrier_name",
        null as "other_payer_2_carrier_code",
        null as "other_payer_2_carrier_name",
        dates.receipt_date as "claim_receipt_date",
        '''' as "subscriber_address2_1",
        split(clm.external_cause_of_injury[1],'':'')[1]::string as "external_cause_code_2",
        split(clm.external_cause_of_injury[2],'':'')[1]::string as "external_cause_code_3",
		    --mem.other_subscriber_id as "other_payer_2_subscriber_policy_num",
		    first_value(case when mem.other_clm_filling_ind_cd in (''MA'',''MB'') THEN mem.other_subscriber_id else null end) OVER (partition by db2.clm_num order by mem.other_subscriber_id ) as "other_payer_2_subscriber_policy_num",
		    split(split(clm.occurrence_span_info[2],'':'')[3],''-'')[0]::string as "occurrence_span_from_3",
        split(split(clm.occurrence_span_info[2],'':'')[3],''-'')[1]::string as "occurrence_span_to_3",
        clm.clm_note_text as "claim_note",
        clm.patient_reason_for_visit_cd[0]::string as "reason_for_visit_1",
        clm.patient_reason_for_visit_cd[1]::string as "reason_for_visit_2",
        clm.patient_reason_for_visit_cd[2]::string as "reason_for_visit_3",
        split(clm.external_cause_of_injury[0],'':'')[8]::string as "external_code_poa_1",
        split(clm.external_cause_of_injury[1],'':'')[8]::string as "external_code_poa_2",
        split(clm.external_cause_of_injury[2],'':'')[8]::string as "external_code_poa_3",
        clm.treatment_cd_info[0]::string as "treatment_code_1",
        clm.treatment_cd_info[1]::string as "treatment_code_2",
        clm.treatment_cd_info[2]::string as "treatment_code_3",
        payer.other_payer_address_1 as "other_payer_1_address1",
        payer.other_payer_address_2 as "other_payer_1_address2",
        payer.other_payer_city as "other_payer_1_city",
        payer.other_payer_state as "other_payer_1_state",
        payer.other_payer_zip as "other_payer_1_zip",
        payer.other_payer_clm_adjust_indicator as "other_payer_1_clm_adj_ind",
        payer.other_payer_clm_ctrl_num as "other_payer_1_clm_ctrl_no",
        -- payer.other_payer_2_address_1 as "other_payer_2_address1",
        -- payer.other_payer_2_address_2 as "other_payer_2_address2",
        -- payer.other_payer_2_city as "other_payer_2_city",
        -- payer.other_payer_2_state as "other_payer_2_state",
        -- payer.other_payer_2_zip as "other_payer_2_zip",
        -- payer.other_payer_2_clm_adjust_indicator as "other_payer_2_clm_adj_ind",
        -- payer.other_payer_2_clm_ctrl_num as "other_payer_2_clm_ctrl_no",
        null as "other_payer_2_address1",
        null as "other_payer_2_address2",
        null as "other_payer_2_city",
        null as "other_payer_2_state",
        null as "other_payer_2_zip",
        null as "other_payer_2_clm_adj_ind",
        null as "other_payer_2_clm_ctrl_no",
        clm.network_trace_number as "clm_network_tracking_number" ,
        mem.subscriber_name_middle as "patient_name_middle",
        mem.subscriber_address2 as "patient_address2",
        clm.other_payer_1_paid_amt as "other_payer_1_paid_amount",
        clm.other_payer_2_paid_amt as "other_payer_2_paid_amt",
        payer.other_payer_prior_auth_num as "auth_number"
        FROM IDENTIFIER(:v_inst_claim_part) clm
        LEFT OUTER JOIN IDENTIFIER(:v_inst_subscriber) mem ON (mem.grp_control_no = clm.grp_control_no
                                                         AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND mem.transactset_create_date = clm.transactset_create_date)
        JOIN IDENTIFIER(:v_tmp_nyapd_clm_filtered) db2 ON (db2.member_id = mem.subscriber_id
                                                        AND db2.clh_trk_id IS NOT NULL
                                                        AND db2.clh_trk_id = clm.network_trace_number
                                                        

        -- Added Date Qualifier
                AND MONTH(TO_TIMESTAMP(db2.clm_recept_dt)) = MONTH(TO_TIMESTAMP(clm.transactset_create_date,''YYYYMMDD'') )
        AND YEAR(TO_TIMESTAMP(db2.clm_recept_dt)) =  YEAR(TO_TIMESTAMP(clm.transactset_create_date,''YYYYMMDD'') ) 
        
        
        
        )
        LEFT OUTER JOIN IDENTIFIER(:v_inst_payer) payer ON (payer.grp_control_no = clm.grp_control_no
                                                         AND payer.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND payer.transactset_create_date = clm.transactset_create_date)
        LEFT OUTER JOIN (SELECT b.grp_control_no,
        b.trancactset_cntl_no,
        b.claim_id	,
        b.transactset_create_date,
        SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.receipt_date)),'''')::string,3,8) as receipt_date,
        ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.hospitalized_admission_date)),'''')::string as hospitalized_admission_date,
        SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.hospitalized_discharge_date)),'''')::string,3,8)  as hospitalized_discharge_date
        FROM
        (SELECT a.grp_control_no,
        a.trancactset_cntl_no,
        a.claim_id,
        a.transactset_create_date,
        
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''050''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as receipt_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''435''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as hospitalized_admission_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''096''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as hospitalized_discharge_date

        
        
        FROM (SELECT distinct grp_control_no,
        trancactset_cntl_no,
        claim_id,
        transactset_create_date,
        OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map
        FROM IDENTIFIER(:v_inst_clm_sv_dates) where sl_seq_num is null and transactset_create_date BETWEEN :v_param_start_date AND :v_param_end_date) a
        group by a.grp_control_no,
        a.trancactset_cntl_no,
        a.claim_id,
        a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                                       AND concat(dates.trancactset_cntl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.claim_id)
                                                       AND dates.transactset_create_date = clm.transactset_create_date) WHERE clm.transactset_create_date BETWEEN :v_param_start_date AND :v_param_end_date) claim;



V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_NYAPD_INSTITUTIONAL_CLAIM) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   



V_STEP := ''STEP3'';
   
 
V_STEP_NAME := ''Generate NYAPD_INST_CLAIM_REPORT''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      


V_STAGE_QUERY1 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/nyapd_plan/''||(SELECT CURRENT_DATE)||''/nyapd_inst_claim_report''||'' FROM (
             SELECT "ucps_clm_num",
"category_of_service",
"place_of_service",
"medical_record_number",
"admission_Date",
"admission_hours",
"admission_source",
"admit_type",
"admit_diagnosis",
"discharge_time",
"patient_status_code",
"subscriber_name_middle",
"subscriber_address2",
"medical_info_release_ind",
"medicare_assign_indicator",
"diagnosis_relating_grp_code",
"diagnosis_code_2",
"diagnosis_code_3",
"procedure_code_1",
"procedure_date_1",
"procedure_code_2",
"procedure_date_2",
"procedure_code_3",
"procedure_date_3",
"Occurrence_span_code",
"occurrence_span_from",
"occurrence_span_to",
"occurrence_span_code_2",
"occurrence_span_from_2",
"occurrence_span_to_2",
"occurrence_span_code_1",
"occurrence_span_code_3",
"occurrence_code_1",
"occurrence_date_1",
"occurrence_code_2",
"occurrence_date_2",
"occurrence_code_3",
"occurrence_date_3",
"condition_code_1",
"condition_code_2",
"condition_code_3",
"value_1_amount",
"value_1_code",
"value_2_amount",
"value_2_code",
"value_3_amount",
"value_3_code",
"external_cause_code",
"diagnosis_present_on_admit_ind_1",
"diagnosis_present_on_admit_ind_2",
"diagnosis_present_on_admit_ind_3",
"other_payer_2_carrier_code",
"other_payer_2_carrier_name",
"claim_receipt_date",
"subscriber_address2_1" as "subscriber_address2",
"external_cause_code_2",
"external_cause_code_3",
"other_payer_2_subscriber_policy_num",
"occurrence_span_from_3",
"occurrence_span_to_3",
"claim_note",
"reason_for_visit_1",
"reason_for_visit_2",
"reason_for_visit_3",
"external_code_poa_1",
"external_code_poa_2",
"external_code_poa_3",
"treatment_code_1",
"treatment_code_2",
"treatment_code_3",
"other_payer_1_address1",
"other_payer_1_address2",
"other_payer_1_city",
"other_payer_1_state",
"other_payer_1_zip",
"other_payer_1_clm_adj_ind",
"other_payer_1_clm_ctrl_no",
"other_payer_2_address1",
"other_payer_2_address2",
"other_payer_2_city",
"other_payer_2_state",
"other_payer_2_zip",
"other_payer_2_clm_adj_ind",
"other_payer_2_clm_ctrl_no",
"clm_network_tracking_number",
"patient_name_middle",
"patient_address2",
"other_payer_1_paid_amount",
"other_payer_2_paid_amt",
"auth_number"
               FROM TMP_NYAPD_INSTITUTIONAL_CLAIM
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


V_STAGE_QUERY2 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/nyapd_plan/''||(SELECT CURRENT_DATE)||''/nyapd_inst_claim_report''||'' FROM (
             SELECT 
            
''''ucps_clm_num'''',
''''category_of_service'''',
''''place_of_service'''',
''''medical_record_number'''',
''''admission_Date'''',
''''admission_hours'''',
''''admission_source'''',
''''admit_type'''',
''''admit_diagnosis'''',
''''discharge_time'''',
''''patient_status_code'''',
''''subscriber_name_middle'''',
''''subscriber_address2'''',
''''medical_info_release_ind'''',
''''medicare_assign_indicator'''',
''''diagnosis_relating_grp_code'''',
''''diagnosis_code_2'''',
''''diagnosis_code_3'''',
''''procedure_code_1'''',
''''procedure_date_1'''',
''''procedure_code_2'''',
''''procedure_date_2'''',
''''procedure_code_3'''',
''''procedure_date_3'''',
''''Occurrence_span_code'''',
''''occurrence_span_from'''',
''''occurrence_span_to'''',
''''occurrence_span_code_2'''',
''''occurrence_span_from_2'''',
''''occurrence_span_to_2'''',
''''occurrence_span_code_1'''',
''''occurrence_span_code_3'''',
''''occurrence_code_1'''',
''''occurrence_date_1'''',
''''occurrence_code_2'''',
''''occurrence_date_2'''',
''''occurrence_code_3'''',
''''occurrence_date_3'''',
''''condition_code_1'''',
''''condition_code_2'''',
''''condition_code_3'''',
''''value_1_amount'''',
''''value_1_code'''',
''''value_2_amount'''',
''''value_2_code'''',
''''value_3_amount'''',
''''value_3_code'''',
''''external_cause_code'''',
''''diagnosis_present_on_admit_ind_1'''',
''''diagnosis_present_on_admit_ind_2'''',
''''diagnosis_present_on_admit_ind_3'''',
''''other_payer_2_carrier_code'''',
''''other_payer_2_carrier_name'''',
''''claim_receipt_date'''',
''''subscriber_address2'''',
''''external_cause_code_2'''',
''''external_cause_code_3'''',
''''other_payer_2_subscriber_policy_num'''',
''''occurrence_span_from_3'''',
''''occurrence_span_to_3'''',
''''claim_note'''',
''''reason_for_visit_1'''',
''''reason_for_visit_2'''',
''''reason_for_visit_3'''',
''''external_code_poa_1'''',
''''external_code_poa_2'''',
''''external_code_poa_3'''',
''''treatment_code_1'''',
''''treatment_code_2'''',
''''treatment_code_3'''',
''''other_payer_1_address1'''',
''''other_payer_1_address2'''',
''''other_payer_1_city'''',
''''other_payer_1_state'''',
''''other_payer_1_zip'''',
''''other_payer_1_clm_adj_ind'''',
''''other_payer_1_clm_ctrl_no'''',
''''other_payer_2_address1'''',
''''other_payer_2_address2'''',
''''other_payer_2_city'''',
''''other_payer_2_state'''',
''''other_payer_2_zip'''',
''''other_payer_2_clm_adj_ind'''',
''''other_payer_2_clm_ctrl_no'''',
''''clm_network_tracking_number'''',
''''patient_name_middle'''',
''''patient_address2'''',
''''other_payer_1_paid_amount'''',
''''other_payer_2_paid_amt'''',
''''auth_number''''
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''*''''
               empty_field_as_null=false
               compression = None 
              )
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
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
