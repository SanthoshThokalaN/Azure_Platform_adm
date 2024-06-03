USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_FWA_INSTITUTIONAL_SERVICE("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME             VARCHAR        default ''FWA_PERFORMANT'';
V_SUB_PROCESS_NAME         VARCHAR        default ''FWA_INSTITUTIONAL_SERVICE'';
V_STEP                     VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER; 
V_ROWS_LOADED              INTEGER; 
V_MESSAGE                  VARCHAR;
V_LAST_QUERY_ID            VARCHAR; 
V_STAGE_QUERY1             VARCHAR; 
V_STAGE_QUERY2             VARCHAR; 
V_PREV_MONTH_START         VARCHAR;
V_PREV_MONTH_END           VARCHAR;
V_CLAIM_RECEIPT_START_DATE VARCHAR;
V_CLAIM_RECEIPT_END_DATE   VARCHAR;

    
V_TMP_FWA_INSTITUTIONAL_SERVICE               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_FWA_INSTITUTIONAL_SERVICE'';

V_TMP_DB2IMPORT_CLM_FILTERED_NEW          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_DB2IMPORT_CLM_FILTERED_NEW'';
    
V_INST_CLAIM_PART          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLAIM_PART'';
V_INST_PROVIDER_ALL        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_PROVIDER_ALL'';
V_INST_CLM_SV_DATES        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLM_SV_DATES'';
V_INST_SUBSCRIBER          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_SUBSCRIBER'';
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
   
V_STEP_NAME := ''Load TMP_FWA_INSTITUTIONAL_SERVICE'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_FWA_INSTITUTIONAL_SERVICE) AS

SELECT DISTINCT service.* FROM
(SELECT
db2.clm_num as "ucps_clm_num",
db2.clm_recept_dt as "extract_key",
line.sl_seq_num as "bill_line_num",
line.product_service_id as "product_service_id",
SPLIT(line.product_service_id_qlfr,'':'')[0]::string as "product_service_id_qlfr",
SPLIT(line.product_service_id_qlfr,'':'')[1]::string as "procedure_cd",
CONCAT_WS(''*'',SPLIT(line.product_service_id_qlfr,'':'')[2],SPLIT(line.product_service_id_qlfr,'':'')[3],SPLIT(line.product_service_id_qlfr,'':'')[4],SPLIT(line.product_service_id_qlfr,'':'')[5]) as "procedure_modifiers",
line.line_item_charge_amt as "line_item_charge_amt",
line.measurement_unit as "measurement_unit",
line.service_unit_count as "service_unit_count",
line.line_item_denied_charge_amt as "line_item_denied_charge_amt",
prvall.sv_operating_phys_map[''prv_name_last'']::string as "operating_prv_name_last",
prvall.sv_operating_phys_map[''prv_name_first'']::string as "operating_prv_name_first",
prvall.sv_operating_phys_map[''prv_name_middle'']::string as "operating_prv_name_middle",
prvall.sv_operating_phys_map[''prv_name_suffix'']::string as "operating_prv_name_suffix",
prvall.sv_operating_phys_map[''prv_id'']::string as "operating_prv_id",
prvall.sv_other_operating_phys_map[''prv_name_last'']::string as "other_operating_phys_name_last",
prvall.sv_other_operating_phys_map[''prv_name_first'']::string as "other_operating_phys_name_first",
prvall.sv_other_operating_phys_map[''prv_name_middle'']::string as "other_operating_phys_name_middle",
prvall.sv_other_operating_phys_map[''prv_name_suffix'']::string as "other_operating_phys_name_suffix",
prvall.sv_other_operating_phys_map[''prv_id'']::string as "other_operating_phys_id",
prvall.sv_rendering_prv_map[''prv_type_qlfr'']::string as "rendering_type_qlfr",
prvall.sv_rendering_prv_map[''prv_name_last'']::string as "rendering_prv_name_last",
prvall.sv_rendering_prv_map[''prv_name_first'']::string as "rendering_prv_name_first",
prvall.sv_rendering_prv_map[''prv_name_middle'']::string as "rendering_prv_name_middle",
prvall.sv_rendering_prv_map[''prv_name_suffix'']::string as "rendering_prv_name_suffix",
prvall.sv_rendering_prv_map[''prv_id'']::string as "rendering_prv_id",
IFF(regexp_instr(dates.service_dates,''-'') > 0, split(dates.service_dates,''-'')[0], dates.service_dates) as "first_date_of_service",
IFF(regexp_instr(dates.service_dates,''-'') > 0, split(dates.service_dates,''-'')[1], dates.service_dates) as "last_date_of_service",
adjudication_or_payment_date as "adjudication_or_payment_date",
line.product_service_id as "service_revenue_code",
line.drug_product_id_qlfr as "drug_product_id_qlfr",
line.drug_product_id as "drug_product_id",
line.drug_unit_count as "drug_unit_count",
line.drug_measure_unit as "drug_measure_unit",
line.cas_adj_group_code as "cas_adj_group_code",
line.cas_adj_reason_code as "cas_adj_reason_code"
FROM IDENTIFIER(:V_INST_PROVIDER_ALL) prvall
JOIN IDENTIFIER(:V_INST_CLAIM_PART) line ON  (line.grp_control_no = prvall.grp_control_no
                                               AND line.trancactset_cntl_no = prvall.trancactset_cntl_no
                                               AND line.claim_id = prvall.claim_id
                                               AND line.sl_seq_num = prvall.sv_lx_number
                                               AND line.transactset_create_date = prvall.transactset_create_date)
LEFT OUTER JOIN (

SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.claim_id	,
b.sl_seq_num,
b.transactset_create_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.service_dates)),'''')::string,3,8)  as service_dates,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.adjudication_or_payment_date)),'''')::string,3,8) as adjudication_or_payment_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.claim_id,
a.sl_seq_num,
a.transactset_create_date,

        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''472''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as service_dates,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''573''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as adjudication_or_payment_date

//ARRAY_AGG(a.group_map[''472'']) as service_dates,
//ARRAY_AGG(a.group_map[''573'']) as adjudication_or_payment_date	


FROM (SELECT grp_control_no,
trancactset_cntl_no,
claim_id,
sl_seq_num,
transactset_create_date,

//OBJECT_INSERT({},clm_date_type,clm_date) as group_map

OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map

FROM IDENTIFIER(:v_inst_clm_sv_dates) where sl_seq_num is not null) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.claim_id,
sl_seq_num,
a.transactset_create_date

) b

) dates
ON (dates.grp_control_no = prvall.grp_control_no
                                               AND dates.trancactset_cntl_no = prvall.trancactset_cntl_no
                                               AND dates.claim_id = prvall.claim_id
                                               AND dates.sl_seq_num = prvall.sv_lx_number
                                               AND dates.transactset_create_date = prvall.transactset_create_date)
JOIN IDENTIFIER(:V_INST_SUBSCRIBER) mem          ON (mem.grp_control_no = line.grp_control_no
                                               AND mem.trancactset_cntl_no = line.trancactset_cntl_no
                                               AND mem.transactset_create_date = line.transactset_create_date)
                                               
JOIN IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_NEW) db2 ON (db2.clh_trk_id = line.network_trace_number
AND YEAR(TO_DATE(db2.clm_recept_dt)) = YEAR(TO_DATE(line.transactset_create_date, ''yyyymmdd''))

)
WHERE 
TO_NUMBER(line.transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END) AND 
line.app_sender_code = ''APTIX''
) service;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_FWA_INSTITUTIONAL_SERVICE)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
V_STEP := ''STEP3'';
   
  
V_STEP_NAME := ''Generate FWA_INST_SERVICE_REPORT''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      



V_STAGE_QUERY1 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_performant/''||''fwa_institutional_service_report_''||(SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYY_MM''))||'' FROM 
(
SELECT *  FROM TMP_FWA_INSTITUTIONAL_SERVICE
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


V_STAGE_QUERY2 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_performant/''||''fwa_institutional_service_report_''||(SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYY_MM''))||'' FROM 
(
SELECT ''''ucps_clm_num'''',
''''extract_key'''',
''''bill_line_num'''' ,
''''product_service_id'''',
''''product_service_id_qlfr'''' ,
''''procedure_cd'''' ,
''''procedure_modifiers'''',
''''line_item_charge_amt'''' ,
''''measurement_unit'''',
''''service_unit_count'''' ,
''''line_item_denied_charge_amt'''',
''''operating_prv_name_last'''' ,
''''operating_prv_name_first'''' ,
''''operating_prv_name_middle'''' ,
''''operating_prv_name_suffix'''' ,
''''operating_prv_id'''' ,
''''other_operating_phys_name_last'''' ,
''''other_operating_phys_name_first'''' ,
''''other_operating_phys_name_middle'''' ,
''''other_operating_phys_name_suffix'''' ,
''''other_operating_phys_id'''' ,
''''rendering_type_qlfr'''' ,
''''rendering_prv_name_last'''' ,
''''rendering_prv_name_first'''' ,
''''rendering_prv_name_middle'''' ,
''''rendering_prv_name_suffix'''' ,
''''rendering_prv_id'''' ,
''''first_date_of_service'''',
''''last_date_of_service'''',
''''adjudication_or_payment_date'''',
''''service_revenue_code'''',
''''drug_product_id_qlfr'''',
''''drug_product_id'''',
''''drug_unit_count'''',
''''drug_measure_unit'''',
''''cas_adj_group_code'''',
''''cas_adj_reason_code''''  
)
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''~''''
			   empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''')
               compression =  ''''gzip''''
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
