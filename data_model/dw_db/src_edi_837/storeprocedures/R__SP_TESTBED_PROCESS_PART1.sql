USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_TESTBED_PROCESS_PART1("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216))
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


V_TESTBED_INPUT_CLAIMS_TMP			varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TESTBED_INPUT_CLAIMS_TMP'';
V_TESTBEDINPUT						varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TESTBEDINPUT'';
V_CH_VIEW                           VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.CH_VIEW'';
V_CMS_TESTBED_DATA					varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.CMS_TESTBED_DATA'';
V_CHP_TESTBED_DATA					varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.CHP_TESTBED_DATA'';
V_CHI_TESTBED_DATA					varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.CHI_TESTBED_DATA'';


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
 
V_STEP_NAME := ''Load Testbed_Input_Claims_tmp''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP()); 


CREATE OR REPLACE TRANSIENT TABLE  IDENTIFIER(:V_TESTBED_INPUT_CLAIMS_TMP)
as
(select $1 as ucps_clm_num,split_part(metadata$filename,''/'',-1) as filename from @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed/CH_CMS/);

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TESTBED_INPUT_CLAIMS_TMP)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);





V_STEP := ''STEP2'';

 
V_STEP_NAME := ''Load VTESTBEDINPUT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

CREATE or replace TRANSIENT TABLE IDENTIFIER(:V_TESTBEDINPUT)
 AS
select distinct clh_trk_id,medicare_clm_cntrl_num,clm_recept_dt,member_id,'''' as "Null",clm_num,clm_type from IDENTIFIER(:V_CH_VIEW) where clm_num in (select ucps_clm_num from IDENTIFIER(:V_TESTBED_INPUT_CLAIMS_TMP));

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TESTBEDINPUT)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);




V_STEP := ''STEP3'';

 
V_STEP_NAME := ''Load CHP_TESTBED_DATA''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

create or replace transient table IDENTIFIER(:V_CHP_TESTBED_DATA) as
select distinct clh_trk_id,clm_recept_dt,member_id,null as "Null",clm_num,clm_type,''D9'' as "claim_type",''CH_837PS_O'' as chp_file_pattern, ''CH_V837PS_O'' as chVp_file_pattern   from IDENTIFIER(:V_TESTBEDINPUT) where clh_trk_id IS NOT NULL and clh_trk_id <> ''null'' and clm_type = ''P'';

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_CHP_TESTBED_DATA)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 

                                 
                                 
                                 
V_STEP := ''STEP4'';

 
V_STEP_NAME := ''LOAD CHI_TESTBED_DATA''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

create or replace transient table IDENTIFIER(:V_CHI_TESTBED_DATA) as
select distinct clh_trk_id,clm_recept_dt,member_id,'''' as "Null",clm_num,clm_type,''D9'' as "claim_type",''CH_837IS_O'' as chi_file_pattern, ''CH_V837IS_O'' as chVi_file_pattern  from IDENTIFIER(:V_TESTBEDINPUT) where clh_trk_id IS NOT NULL and clh_trk_id <> ''null'' and clm_type = ''I'';

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_CHI_TESTBED_DATA)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


V_STEP := ''STEP5'';

 
V_STEP_NAME := ''LOAD  CMS_TESTBED_DATA''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

create or replace transient table  IDENTIFIER(:V_CMS_TESTBED_DATA) as select distinct medicare_clm_cntrl_num,to_varchar(clm_recept_dt::date,''YYYYMMDD'') as ucps_clm_part_start_date,to_varchar(dateadd(day,21,clm_recept_dt)::date,''YYYYMMDD'') as ucps_clm_part_end_date,member_id,'''' as "Null",clm_num,clm_type,''D9'' as "claim_type",''CMS_837P_O'' as file_pattern from IDENTIFIER(:V_TESTBEDINPUT) where medicare_clm_cntrl_num IS NOT NULL and clm_type = ''P'' and member_id <> ''null'' and medicare_clm_cntrl_num <> ''null'';

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_CMS_TESTBED_DATA)) ;

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