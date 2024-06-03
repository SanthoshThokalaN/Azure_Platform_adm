USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_FEK_TESTBED_PROCESS_PART1("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME             VARCHAR        default ''TESTBED PROCESS'';
V_SUB_PROCESS_NAME         VARCHAR        default ''TESTBED CLAIM GENERATION'';
V_STEP                     VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER; 
V_ROWS_LOADED              INTEGER; 
V_MESSAGE                  VARCHAR;
V_LAST_QUERY_ID            VARCHAR; 
V_STAGE_QUERY              VARCHAR; 


V_TESTBED_INPUT_CLAIMS_TMP			varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TESTBED_FEK_INPUT_CLAIMS_TMP'';
V_TESTBED_INPUT_FEK_TMP			    varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TESTBED_INPUT_FEK_TMP'';
V_TESTBEDFEK_DOC_CTL_NBR			varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TESTBEDFEK_DOC_CTL_NBR'';
V_FOX_IMPORT                        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.FOXIMPORT'';
V_FEKP_TESTBED_DATA					varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.FEKP_TESTBED_DATA'';
V_FEKI_TESTBED_DATA					varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.FEKI_TESTBED_DATA'';
    

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';


V_STEP := ''STEP1'';
 
V_STEP_NAME := ''Load FEK Testbed_Input_Claims_tmp''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP()); 


CREATE OR REPLACE TRANSIENT TABLE  IDENTIFIER(:V_TESTBED_INPUT_CLAIMS_TMP)
as
(select $1 as ucps_clm_num, $2 as clm_type, substr(metadata$filename, 31, 60) as file from @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed/FEK);

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TESTBED_INPUT_CLAIMS_TMP)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 


V_STEP := ''STEP2'';

 
V_STEP_NAME := ''Load Testbed FEK Temp Table''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

CREATE or replace TEMPORARY TABLE IDENTIFIER(:V_TESTBED_INPUT_FEK_TMP)
AS
SELECT substr(ucps_clm_num,1,12) as ref_id_value,
CASE 
WHEN ucps_clm_num LIKE ''0%'' THEN CONCAT_WS('''',''2020'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''9%'' THEN CONCAT_WS('''',''2019'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''1%'' THEN CONCAT_WS('''',''2021'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''2%'' THEN CONCAT_WS('''',''2022'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''3%'' THEN CONCAT_WS('''',''2023'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''4%'' THEN CONCAT_WS('''',''2024'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''5%'' THEN CONCAT_WS('''',''2025'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''6%'' THEN CONCAT_WS('''',''2026'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''7%'' THEN CONCAT_WS('''',''2027'',substr(ucps_clm_num,2,3))
WHEN ucps_clm_num LIKE ''8%'' THEN CONCAT_WS('''',''2028'',substr(ucps_clm_num,2,3)) END as ucps_clm_dt,
''11111111111'' as member_id,
''9999999999'' as clm_info_key,
substr(ucps_clm_num,1,12) as ucps_clm_num,
clm_type as clm_type,
CASE when clm_type = ''I'' THEN ''D9''
when clm_type = ''P'' THEN ''F8'' END as ref_id_qlfr,
substr(ucps_clm_num,2,3) as fox_clm from IDENTIFIER(:V_TESTBED_INPUT_CLAIMS_TMP);

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TESTBED_INPUT_FEK_TMP)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);






V_STEP := ''STEP3'';

 
V_STEP_NAME := ''LOAD FEK_DOC_CNTL_NBR data''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TESTBEDFEK_DOC_CTL_NBR) AS
SELECT 
b.doc_ctl_nbr ,
b.clm_recpt_dt AS ucps_clm_dt  ,
a.member_id ,
a.clm_info_key ,
a.ucps_clm_num ,
a.clm_type ,
a.ref_id_qlfr  FROM IDENTIFIER(:V_TESTBED_INPUT_FEK_TMP) a
JOIN
IDENTIFIER(:V_FOX_IMPORT) b on a.ucps_clm_num= b.clm_num and a.fox_clm>=500 and a.fox_clm <=900;



V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TESTBEDFEK_DOC_CTL_NBR)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);








V_STEP := ''STEP4'';

 
V_STEP_NAME := ''LOAD fekP doc cntl testbed data''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

CREATE OR REPLACE TRANSIENT TABLE IDENTIFIER(:V_FEKP_TESTBED_DATA) AS
SELECT DISTINCT 
doc_ctl_nbr,
ucps_clm_dt,
member_id,
clm_info_key,
ucps_clm_num,
clm_type,
ref_id_qlfr,
''FEK_837PS_O'' AS file_pattern FROM IDENTIFIER(:V_TESTBEDFEK_DOC_CTL_NBR) 
WHERE clm_type = ''P'' AND doc_ctl_nbr <> ''null'';


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_FEKP_TESTBED_DATA)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);





V_STEP := ''STEP5'';

 
V_STEP_NAME := ''LOAD fekI doc cntl testbed data''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

CREATE OR REPLACE TRANSIENT TABLE IDENTIFIER(:V_FEKI_TESTBED_DATA) AS
SELECT DISTINCT 
doc_ctl_nbr,
ucps_clm_dt,
member_id,
clm_info_key,
ucps_clm_num,
clm_type,
ref_id_qlfr,
''FEK_837IS_O'' AS file_pattern FROM IDENTIFIER(:V_TESTBEDFEK_DOC_CTL_NBR) 
WHERE clm_type = ''I'' AND doc_ctl_nbr <> ''null'';



V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_FEKI_TESTBED_DATA)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);






EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';