-- sree notes
USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_837_BRIDGE_SOLUTION"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''837_BRIDGE_SOLUTION'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''837_BRIDGE_SOLUTION'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;

V_TMP_BRIDGE_837_INST_DATA       VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''ADR_PRE_CLM'') || ''.TMP_BRIDGE_837_INST_DATA'';
V_TMP_BRIDGE_837_PROF_DATA       VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''ADR_PRE_CLM'') || ''.TMP_BRIDGE_837_PROF_DATA'';
    
    
V_INST_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLAIM_PART'';
V_PROF_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLAIM_PART'';
V_PROF_SUBSCRIBER            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_SUBSCRIBER'';
V_FOXIMPORT                  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.FOXIMPORT'';





BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
   
 
V_STEP_NAME := ''Load TMP_BRIDGE_837_INST_DATA''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


    
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_BRIDGE_837_INST_DATA) AS 


SELECT
    DISTINCT foximp.clm_num as clm_num,
    transact_type_code,
    healthcareservice_location,
    admit_type_code,
    admit_source_code,
    patient_status_code,
    health_care_code_info,
    sv_clm_payment_remark_code,
    product_service_id_qlfr,
    product_service_id,
    measurement_unit,
    service_unit_count ,
    cas_adj_group_code,
    cas_adj_reason_code,
    cas_adj_amt,
    principal_procedure_info,
    hc_condition_codes as HC_Condition_Code,
    clm_admitting_diagnosis_cd,
    patient_reason_for_visit_cd as Patient_Reason_for_Visit_Code,
    diagnosis_related_grp_info
FROM
    IDENTIFIER(:V_INST_CLAIM_PART) clm
    INNER JOIN IDENTIFIER(:v_foximport) foximp ON (
        foximp.clh_trk_id = clm.network_trace_number
        AND foximp.clh_trk_id NOT LIKE ''null''
        AND foximp.clh_trk_id IS NOT NULL
        AND foximp.clm_stat in (''D'', ''R'') --condition to filter claims with D & R claim status
    )
WHERE
    SUBSTR(foximp.CLM_NUM, 12, 1) <> ''0'' --condition to filer split-0 claims i.e claim number ends with 0
    AND TRIM(SUBSTR(foximp.dt_cmpltd, 1, 10)) BETWEEN TO_VARCHAR(DATEADD(MONTH, -1, :V_CURRENT_DATE)::date,''YYYY-MM-01'') 
    AND TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, :V_CURRENT_DATE))::date,''YYYY-MM-DD'')
    AND clm.transactset_create_date BETWEEN  TO_VARCHAR(DATEADD(year, -2, :V_CURRENT_DATE)::date,''YYYYMMDD'') AND to_varchar((:V_CURRENT_DATE+1)::date,''YYYYMMDD'') --date range on transactset_create_dt has been used to optimize the query
UNION
SELECT
    DISTINCT foximp.clm_num as clm_num,
    transact_type_code,
    healthcareservice_location,
    admit_type_code,
    admit_source_code,
    patient_status_code,
    health_care_code_info,
    sv_clm_payment_remark_code,
    product_service_id_qlfr,
    product_service_id,
    measurement_unit,
    service_unit_count ,
    cas_adj_group_code,
    cas_adj_reason_code,
    cas_adj_amt,
    principal_procedure_info,
    hc_condition_codes as HC_Condition_Code,
    clm_admitting_diagnosis_cd,
    patient_reason_for_visit_cd as Patient_Reason_for_Visit_Code,
    diagnosis_related_grp_info
FROM
    IDENTIFIER(:V_INST_CLAIM_PART) clm
    INNER JOIN IDENTIFIER(:v_foximport) foximp ON (
        foximp.doc_ctl_nbr = TRIM(
            SUBSTR(split(clm.clm_billing_note_text, '' '') [1], 3, 12)
        )
        AND (clm.app_sender_code = ''EXELA'')
        AND (
            foximp.doc_ctl_nbr NOT LIKE ''null''
            AND foximp.doc_ctl_nbr IS NOT NULL
        )
        AND foximp.clm_stat in (''D'', ''R'')
    )
WHERE
    SUBSTR(foximp.CLM_NUM, 12, 1) <> ''0''
    AND TRIM(SUBSTR(foximp.dt_cmpltd, 1, 10)) BETWEEN TO_VARCHAR(DATEADD(MONTH, -1, :V_CURRENT_DATE)::date,''YYYY-MM-01'') 
    AND TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, :V_CURRENT_DATE))::date,''YYYY-MM-DD'')
    AND clm.transactset_create_date BETWEEN TO_VARCHAR(DATEADD(year, -2, :V_CURRENT_DATE)::date,''YYYYMMDD'') AND to_varchar((:V_CURRENT_DATE+1)::date,''YYYYMMDD'');



    

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_BRIDGE_837_INST_DATA)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


   V_STEP := ''STEP2'';
   
   V_STEP_NAME := ''Load TMP_BRIDGE_837_PROF_DATA''; 
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

   
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_BRIDGE_837_PROF_DATA) AS  



SELECT
    DISTINCT foximp.clm_num as clm_num,
    clm.transact_type_code,
    healthcareservice_location,
    health_care_code_info,
    product_service_id_qlfr,
    sv_clm_payment_remark_code,
    measurement_unit,
    service_unit_count,
    cas_adj_group_code,
    cas_adj_reason_code,
    cas_adj_amt,
    hc_condition_codes as HC_Condition_Code,
    vendor_cd,
    health_care_additional_code_info,
    diagnosis_code_pointers
FROM
    IDENTIFIER(:V_PROF_CLAIM_PART) clm
    INNER JOIN IDENTIFIER(:v_prof_subscriber) sub ON (
        sub.grp_control_no = clm.grp_control_no
        AND sub.trancactset_cntl_no = clm.trancactset_cntl_no
        AND sub.subscriber_hl_no = clm.subscriber_hl_no
        AND sub.transactset_create_date = clm.transactset_create_date
        AND sub.xml_md5 = clm.xml_md5
    )
    INNER JOIN IDENTIFIER(:v_foximport) foximp ON (
        foximp.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num
        AND foximp.orig_mbr_nbr = sub.subscriber_id --this join is mandatory for CMS records
    )
where
    foximp.medcr_clm_ctl_nbr IS NOT NULL
    AND clm.vendor_cd = ''CMS''
    AND foximp.clm_stat in (''D'', ''R'')
    AND SUBSTR(foximp.CLM_NUM, 12, 1) <> ''0''
    AND TRIM(SUBSTR(foximp.dt_cmpltd, 1, 10)) BETWEEN TO_VARCHAR(DATEADD(MONTH, -1, :V_CURRENT_DATE)::date,''YYYY-MM-01'') 
    AND TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, :V_CURRENT_DATE))::date,''YYYY-MM-DD'')
    AND clm.transactset_create_date BETWEEN TO_VARCHAR(DATEADD(year, -2, :V_CURRENT_DATE)::date,''YYYYMMDD'') AND to_varchar((:V_CURRENT_DATE+1)::date,''YYYYMMDD'')
UNION
SELECT
    DISTINCT foximp.clm_num as clm_num,
    transact_type_code,
    healthcareservice_location,
    health_care_code_info,
    product_service_id_qlfr,
    sv_clm_payment_remark_code,
    measurement_unit,
    service_unit_count,
    cas_adj_group_code,
    cas_adj_reason_code,
    cas_adj_amt,
    hc_condition_codes as HC_Condition_Code,
    vendor_cd,
    health_care_additional_code_info,
    diagnosis_code_pointers
FROM
    IDENTIFIER(:V_PROF_CLAIM_PART) clm
    INNER JOIN IDENTIFIER(:v_foximport) foximp ON (foximp.clh_trk_id = clm.network_trace_number)
where
    foximp.clh_trk_id NOT LIKE ''null''
    AND foximp.clh_trk_id IS NOT NULL
    AND clm.vendor_cd = ''CH''
    AND foximp.clm_stat in (''D'', ''R'')
    AND SUBSTR(foximp.CLM_NUM, 12, 1) <> ''0''
    AND TRIM(SUBSTR(foximp.dt_cmpltd, 1, 10)) BETWEEN TO_VARCHAR(DATEADD(MONTH, -1, :V_CURRENT_DATE)::date,''YYYY-MM-01'') 
    AND TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, :V_CURRENT_DATE))::date,''YYYY-MM-DD'')
    AND clm.transactset_create_date BETWEEN TO_VARCHAR(DATEADD(year, -2, :V_CURRENT_DATE)::date,''YYYYMMDD'') AND to_varchar((:V_CURRENT_DATE+1)::date,''YYYYMMDD'')
UNION
SELECT
    DISTINCT foximp.clm_num as clm_num,
    transact_type_code,
    healthcareservice_location,
    health_care_code_info,
    product_service_id_qlfr,
    sv_clm_payment_remark_code,
    measurement_unit,
    service_unit_count,
    cas_adj_group_code,
    cas_adj_reason_code,
    cas_adj_amt,
    hc_condition_codes as HC_Condition_Code,
    vendor_cd,
    health_care_additional_code_info,
    diagnosis_code_pointers
FROM
    IDENTIFIER(:V_PROF_CLAIM_PART) clm
    INNER JOIN IDENTIFIER(:v_foximport) foximp ON (
        foximp.doc_ctl_nbr = TRIM(
            SUBSTR(split(clm.clm_billing_note_text, '' '') [1], 3, 12)
        )
        AND (clm.app_sender_code = ''EXELA'')
        AND (
            foximp.doc_ctl_nbr NOT LIKE ''null''
            AND foximp.doc_ctl_nbr IS NOT NULL
        )
        AND foximp.clm_stat in (''D'', ''R'')
    )
WHERE
    SUBSTR(foximp.CLM_NUM, 12, 1) <> ''0''
    AND TRIM(SUBSTR(foximp.dt_cmpltd, 1, 10)) BETWEEN TO_VARCHAR(DATEADD(MONTH, -1, :V_CURRENT_DATE)::date,''YYYY-MM-01'') 
    AND TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, :V_CURRENT_DATE))::date,''YYYY-MM-DD'')
    AND clm.transactset_create_date BETWEEN TO_VARCHAR(DATEADD(year, -2, :V_CURRENT_DATE)::date,''YYYYMMDD'') AND to_varchar((:V_CURRENT_DATE+1)::date,''YYYYMMDD'');
        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_BRIDGE_837_PROF_DATA)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
                                 
V_STEP := ''STEP3'';

 
V_STEP_NAME := ''Generate File for 837 Inst Data''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/837_bridge_solution/''||''837_Bridge_Inst_Data''||''.csv.gz''||'' FROM (
             SELECT *
               FROM TMP_BRIDGE_837_INST_DATA
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''|''''
               compression = ''''gzip''''
                            
               )
               
               
               
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

execute immediate ''USE SCHEMA ''||:TGT_SC;                          
execute immediate :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);                                 
                                 
V_STEP := ''STEP4'';

 
V_STEP_NAME := ''Generate File for 837 Prof Data''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/837_bridge_solution/''||''837_Bridge_Prof_Data''||''.csv.gz''||'' FROM (
             
             
             
             SELECT *
               FROM TMP_BRIDGE_837_PROF_DATA
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''|''''
               compression = ''''gzip''''
              )
               
               
               
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

execute immediate ''USE SCHEMA ''||:TGT_SC;                          
execute immediate :V_STAGE_QUERY;  


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
