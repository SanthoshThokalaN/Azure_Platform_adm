USE SCHEMA SRC_EDI_837;
  
CREATE OR REPLACE PROCEDURE "SP_NYAPD_INSTITUTIONAL_SERVICE"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "LZ_ISDW_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''NYAPD_PLAN'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''NYAPD_INSTITUTIONAL_SERVICE'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY1      VARCHAR;
V_STAGE_QUERY2     VARCHAR;
V_STAGE_QUERY3		VARCHAR;

V_PARAM_START_DATE VARCHAR ;
V_PARAM_END_DATE   VARCHAR ;


V_TMP_NYAPD_CLM_FILTERED   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_NYAPD_CLM_FILTERED'';

V_TMP_NYAPD_INSTITUTIONAL_SERVICE  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_NYAPD_INSTITUTIONAL_SERVICE'';



V_NYAPD_CLAIM_STG            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.NYAPD_CLAIM_STG'';
V_INST_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLAIM_PART'';

V_INST_SUBSCRIBER            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_SUBSCRIBER'';

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

SELECT db2.* FROM IDENTIFIER(:V_CH_VIEW) db2 JOIN IDENTIFIER(:V_NYAPD_CLAIM_STG) nyapd on db2.clm_num = nyapd.clm_num

;

V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_NYAPD_CLM_FILTERED) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);    


V_STEP := ''STEP2'';
 
V_STEP_NAME := ''Load TMP_NYAPD_INSTITUTIONAL_SERVICE''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_NYAPD_INSTITUTIONAL_SERVICE) AS 

      SELECT DISTINCT service.* from (SELECT
        db2.clm_num as "ucps_clm_num",
        clm.sl_seq_num as "bill_line_num",
        clm.product_service_id as "revenue_code",
        split(clm.product_service_id_qlfr,'':'')[2]::string as "procedure_modifier_1",
        split(clm.product_service_id_qlfr,'':'')[3]::string as "procedure_modifier_2",
        clm.service_unit_count as "service_unit_quantity",
        clm.drug_product_id as "ndc_code",
        clm.drug_measure_unit as "ndc_unit_measure",
        CASE WHEN split(RTRIM(clm.drug_unit_count,''0''),''.'')[1]='''' THEN RTRIM(clm.drug_unit_count,''0'')||''0'' ELSE RTRIM(clm.drug_unit_count,''0'') END  as "ndc_unit_quantity",
        split(clm.product_service_id_qlfr,'':'')[4]::string as "procedure_modifier_3",
        split(clm.product_service_id_qlfr,'':'')[5]::string as "procedure_modifier_4"
        FROM identifier(:v_inst_claim_part) clm
        LEFT OUTER JOIN identifier(:v_inst_subscriber) mem ON (mem.grp_control_no = clm.grp_control_no
                                                         AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                         AND mem.transactset_create_date = clm.transactset_create_date)
        JOIN identifier(:v_tmp_nyapd_clm_filtered) db2 ON (db2.member_id = mem.subscriber_id
                                                        AND db2.clh_trk_id IS NOT NULL
                                                        AND db2.clh_trk_id = clm.network_trace_number
                                                        
        -- Added Date qualifier                                                
                AND MONTH(TO_TIMESTAMP(db2.clm_recept_dt)) = MONTH(TO_TIMESTAMP(clm.transactset_create_date,''YYYYMMDD'') )
        AND YEAR(TO_TIMESTAMP(db2.clm_recept_dt)) =  YEAR(TO_TIMESTAMP(clm.transactset_create_date,''YYYYMMDD'') ) 
        
        
        )
        WHERE clm.transactset_create_date BETWEEN :v_param_start_date AND :v_param_end_date ) service;



V_ROWS_LOADED := (select count(1)  from IDENTIFIER(:V_TMP_NYAPD_INSTITUTIONAL_SERVICE) ) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);       



V_STEP := ''STEP3'';
   
 
V_STEP_NAME := ''Generate NYAPD_INST_SERVICE_REPORT''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      


V_STAGE_QUERY1 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/nyapd_plan/''||(SELECT CURRENT_DATE)||''/nyapd_inst_service_report''||'' FROM (
             SELECT *
               FROM TMP_NYAPD_INSTITUTIONAL_SERVICE
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

V_STAGE_QUERY2 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/nyapd_plan/''||(SELECT CURRENT_DATE)||''/nyapd_inst_service_report''||'' FROM (
             SELECT
		       ''''ucps_clm_num'''',
               ''''bill_line_num'''',
               ''''revenue_code'''',
               ''''procedure_modifier_1'''',
               ''''procedure_modifier_2'''',
               ''''service_unit_quantity'''',
               ''''ndc_code'''',
               ''''ndc_unit_measure'''',
               ''''ndc_unit_quantity'''',
               ''''procedure_modifier_3'''',
               ''''procedure_modifier_3''''

               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''*''''
			   empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''')
               compression = None 
              )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;

V_STAGE_QUERY3 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/nyapd_plan/''||(SELECT CURRENT_DATE)||''/nyapd_edp_report_complete''||'' FROM (
             SELECT
		       to_varchar(current_timestamp,''''YYYY-MM-DD-HH24-MI'''')

               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''*''''
			   empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''')
               compression = None 
              )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;


IF (V_ROWS_LOADED > 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY1;
execute immediate  :V_STAGE_QUERY3;
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

ELSEIF (V_ROWS_LOADED = 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY2;  
execute immediate  :V_STAGE_QUERY3;
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
