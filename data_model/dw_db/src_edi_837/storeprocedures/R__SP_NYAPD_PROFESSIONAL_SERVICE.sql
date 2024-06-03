USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE "SP_NYAPD_PROFESSIONAL_SERVICE"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "LZ_ISDW_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''NYAPD_PLAN'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''NYAPD_PROFESSIONAL_SERVICE'';

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


V_TMP_NYAPD_CLM_FILTERED   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_NYAPD_CLM_FILTERED'';
V_TMP_PROF_SERVICE_NYAPD   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PROF_SERVICE_NYAPD'';
V_TMP_NYAPD_PROFESSIONAL_SERVICE  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_NYAPD_PROFESSIONAL_SERVICE'';



V_NYAPD_CLAIM_STG            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.NYAPD_CLAIM_STG'';
V_PROF_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLAIM_PART'';

V_PROF_SERVICE                VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_SERVICE'';

V_PROF_SUBSCRIBER            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_SUBSCRIBER'';
V_PROF_CLM_SV_DATES           VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLM_SV_DATES'';
V_CH_VIEW                    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.CH_VIEW'';


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

V_PARAM_START_DATE := (SELECT to_varchar(DATEADD(day,-10,MIN(UCPS_CLM_DT)),''YYYYMMDD'') FROM   IDENTIFIER(:V_NYAPD_CLAIM_STG));
V_PARAM_END_DATE :=   (SELECT to_varchar(MAX(UCPS_CLM_DT)::DATE,''YYYYMMDD'') FROM   IDENTIFIER(:V_NYAPD_CLAIM_STG) WHERE UCPS_CLM_DT IS NOT NULL);



ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
 
V_STEP_NAME := ''Load TMP_NYAPD_CLM_FILTERED ''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());



CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) AS

SELECT db2.* FROM IDENTIFIER(:V_CH_VIEW) db2 JOIN IDENTIFIER(:V_NYAPD_CLAIM_STG) nyapd on db2.clm_num = nyapd.clm_num

;


V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   
                                 
V_STEP := ''STEP2'';
 
V_STEP_NAME := ''Load TMP_PROF_SERVICE_NYAPD ''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 


CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_PROF_SERVICE_NYAPD) AS

SELECT DISTINCT service.* FROM (
        SELECT
        db2.clm_num as ucps_clm_num,
        clm.sl_seq_num as bill_line_num,
        clm.epsdt_indicator as epsdt_indicator,
        dates.adjudication_or_payment_date as adjudication_or_payment_date,
        SPLIT(line.medical_procedure_id,'':'')[2]::string as procedure_modifier1,
        SPLIT(line.medical_procedure_id,'':'')[3]::string as procedure_modifier2,
        SPLIT(line.medical_procedure_id,'':'')[4]::string as procedure_modifier3,
        clm.service_unit_count as service_unit_count,
        clm.emergency_indicator as emergency_service_ind,
        clm.family_planning_indicator as family_planning_ind,
        SPLIT(clm.diagnosis_code_pointers,'':'')[1]::string as diagnosis_cd_pointer2,
        SPLIT(clm.diagnosis_code_pointers,'':'')[2]::string as diagnosis_cd_pointer3,
        FIRST_VALUE(clm.cas_adj_reason_code) OVER (PARTITION BY db2.clm_num,line.sl_seq_num ORDER BY db2.clm_num,line.sl_seq_num) as adjustment_reason_code,
        line.drug_product_id as ndc_code,
        line.drug_measure_unit as ndc_unit_measure,
        line.drug_unit_count as ndc_unit_quantity
        FROM IDENTIFIER(:v_prof_claim_part) clm LEFT OUTER JOIN IDENTIFIER(:v_prof_subscriber) mem ON (mem.grp_control_no = clm.grp_control_no
                                                         AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                         AND mem.transactset_create_date = clm.transactset_create_date
                                                         AND mem.xml_md5 = clm.xml_md5 )
        JOIN IDENTIFIER(:v_tmp_nyapd_clm_filtered) db2 ON (db2.member_id = mem.subscriber_id
                                                         AND db2.medicare_clm_cntrl_num IS NOT NULL
                                                         AND db2.medicare_clm_cntrl_num = clm.payer_clm_ctrl_num
                                                         
                                                         

        -- Added Date qualifier
        AND MONTH(TO_TIMESTAMP(db2.clm_recept_dt)) = MONTH(TO_TIMESTAMP(clm.transactset_create_date,''YYYYMMDD'') )
        AND YEAR(TO_TIMESTAMP(db2.clm_recept_dt)) =  YEAR(TO_TIMESTAMP(clm.transactset_create_date,''YYYYMMDD'') ) 
        
        
        )
        LEFT OUTER JOIN (SELECT b.grp_control_no,
        b.trancactset_cntl_no,
        b.provider_hl_no,
        b.subscriber_hl_no,
        b.payer_hl_no	,
        b.claim_id	,
        b.sl_seq_num,
        b.transactset_create_date,
        b.adjudication_or_payment_date as adjudication_or_payment_date
        FROM
        (SELECT a.grp_control_no,
        a.trancactset_cntl_no,
        a.provider_hl_no,
        a.subscriber_hl_no,
        a.payer_hl_no,
        a.claim_id,
        a.sl_seq_num,
        a.transactset_create_date,
        PARSE_JSON(MAX(a.group_map[''573''])::ARRAY[0])::string as adjudication_or_payment_date
        
        FROM (SELECT distinct grp_control_no,
        trancactset_cntl_no,
        provider_hl_no,
        subscriber_hl_no,
        payer_hl_no,
        claim_id,
        sl_seq_num,
        transactset_create_date,
        OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map
        FROM IDENTIFIER(:v_prof_clm_sv_dates) where sl_seq_num is not null) a
        group by a.grp_control_no,
        a.trancactset_cntl_no,
        a.provider_hl_no,
        a.subscriber_hl_no,
        a.payer_hl_no,
        a.claim_id,
        sl_seq_num,
        a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                                       AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id,''$'',dates.sl_seq_num) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
                                                       AND dates.transactset_create_date = clm.transactset_create_date)
        LEFT OUTER JOIN IDENTIFIER(:v_prof_service) line ON (line.grp_control_no = clm.grp_control_no
                                                       AND concat(line.trancactset_cntl_no,''$'',line.provider_hl_no,''$'',line.subscriber_hl_no,''$'',line.claim_id,''$'',line.sl_seq_num) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
                                                       AND line.transactset_create_date = clm.transactset_create_date) WHERE clm.transactset_create_date BETWEEN :v_param_start_date AND :v_param_end_date
        UNION ALL
        SELECT
        db2.clm_num as ucps_clm_num,
        clm.sl_seq_num as bill_line_num,
        clm.epsdt_indicator as epsdt_indicator,
        dates.adjudication_or_payment_date as adjudication_or_payment_date,
        SPLIT(line.medical_procedure_id,'':'')[2]::string as procedure_modifier1,
        SPLIT(line.medical_procedure_id,'':'')[3]::string as procedure_modifier2,
        SPLIT(line.medical_procedure_id,'':'')[4]::string as procedure_modifier3,
        clm.service_unit_count as service_unit_count,
        clm.emergency_indicator as emergency_service_ind,
        clm.family_planning_indicator as family_planning_ind,
        SPLIT(clm.diagnosis_code_pointers,'':'')[1]::string as diagnosis_cd_pointer2,
        SPLIT(clm.diagnosis_code_pointers,'':'')[2]::string as diagnosis_cd_pointer3,
        FIRST_VALUE(clm.cas_adj_reason_code) OVER (PARTITION BY db2.clm_num,line.sl_seq_num ORDER BY db2.clm_num,line.sl_seq_num) as adjustment_reason_code,
        line.drug_product_id as ndc_code,
        line.drug_measure_unit as ndc_unit_measure,
        line.drug_unit_count as ndc_unit_quantity
        FROM IDENTIFIER(:v_prof_claim_part) clm LEFT OUTER JOIN IDENTIFIER(:v_prof_subscriber) mem ON (mem.grp_control_no = clm.grp_control_no
                                                         AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                         AND mem.transactset_create_date = clm.transactset_create_date
                                                         AND mem.xml_md5 = clm.xml_md5)
        JOIN IDENTIFIER(:v_tmp_nyapd_clm_filtered)db2 ON (db2.member_id = mem.subscriber_id
                                                         AND db2.clh_trk_id IS NOT NULL
                                                         AND db2.clh_trk_id = clm.network_trace_number)
        LEFT OUTER JOIN (SELECT b.grp_control_no,
        b.trancactset_cntl_no,
        b.provider_hl_no,
        b.subscriber_hl_no,
        b.payer_hl_no	,
        b.claim_id	,
        b.sl_seq_num,
        b.transactset_create_date,
        b.adjudication_or_payment_date as adjudication_or_payment_date
        FROM
        (SELECT a.grp_control_no,
        a.trancactset_cntl_no,
        a.provider_hl_no,
        a.subscriber_hl_no,
        a.payer_hl_no,
        a.claim_id,
        a.sl_seq_num,
        a.transactset_create_date,
        PARSE_JSON(MAX(a.group_map[''573''])::ARRAY[0])::string as adjudication_or_payment_date
        FROM (SELECT distinct grp_control_no,
        trancactset_cntl_no,
        provider_hl_no,
        subscriber_hl_no,
        payer_hl_no,
        claim_id,
        sl_seq_num,
        transactset_create_date,
        OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map
        FROM IDENTIFIER(:v_prof_clm_sv_dates) where sl_seq_num is not null) a
        group by a.grp_control_no,
        a.trancactset_cntl_no,
        a.provider_hl_no,
        a.subscriber_hl_no,
        a.payer_hl_no,
        a.claim_id,
        sl_seq_num,
        a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                                       AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id,''$'',dates.sl_seq_num) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
                                                       AND dates.transactset_create_date = clm.transactset_create_date)
        LEFT OUTER JOIN IDENTIFIER(:v_prof_service) line ON (line.grp_control_no = clm.grp_control_no
                                                       AND concat(line.trancactset_cntl_no,''$'',line.provider_hl_no,''$'',line.subscriber_hl_no,''$'',line.claim_id,''$'',line.sl_seq_num) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
                                                       AND line.transactset_create_date = clm.transactset_create_date) WHERE clm.transactset_create_date BETWEEN :v_param_start_date AND :v_param_end_date) service;

;

V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_PROF_SERVICE_NYAPD) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   



V_STEP := ''STEP3'';
 
V_STEP_NAME := ''Load TMP_NYAPD_PROFESSIONAL_SERVICE ''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());    


ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE; 
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_NYAPD_PROFESSIONAL_SERVICE) AS 

        select ucps_clm_num as "ucps_clm_num",
        bill_line_num as "bill_line_num",
        epsdt_indicator as "epsdt_indicator",
        adjudication_or_payment_date as "adjudication_or_payment_date",
        procedure_modifier1 as "procedure_modifier1",
        procedure_modifier2 as "procedure_modifier2",
        procedure_modifier3 as "procedure_modifier3",
        service_unit_count as "service_unit_count",
        emergency_service_ind as "emergency_service_ind",
        family_planning_ind as "family_planning_ind",
        diagnosis_cd_pointer2 "diagnosis_cd_pointer2",
        diagnosis_cd_pointer3 as "diagnosis_cd_pointer3",
        adjustment_reason_code as "adjustment_reason_code",
        ndc_code as "ndc_code",
        ndc_unit_measure as "ndc_unit_measure",
        ndc_unit_quantity as "ndc_unit_quantity" from (
        select m.*,
        ROW_NUMBER() OVER (PARTITION BY ucps_clm_num ORDER BY bill_line_num ASC) AS RN
        from IDENTIFIER(:V_tmp_prof_service_nyapd) m) m2 WHERE m2.RN = 1;



V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_NYAPD_PROFESSIONAL_SERVICE) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);     




V_STEP := ''STEP4'';
   
 
V_STEP_NAME := ''Generate NYAPD_PROF_SERVICE_REPORT''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      


V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/nyapd_plan/''||(SELECT CURRENT_DATE)||''/nyapd_prof_service_report''||'' FROM (
             SELECT *
               FROM TMP_NYAPD_PROFESSIONAL_SERVICE
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