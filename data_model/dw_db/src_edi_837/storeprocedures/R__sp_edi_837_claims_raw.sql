USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_EDI_837_CLAIMS_RAW"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "FILE_PATTERN" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "FILE_FORMAT" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_PROF_QUERY VARCHAR;
V_INST_QUERY VARCHAR;
V_ROWS_PARSED INTEGER DEFAULT 0;
V_ROWS_LOADED INTEGER DEFAULT 0;
V_MESSAGE VARCHAR DEFAULT ''Copy executed with 0 files processed.'';
V_LAST_QUERY_ID VARCHAR;
V_LAST_QUERY_ID_CNT INTEGER;
V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER'';
V_SUB_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER'';
V_STEP             VARCHAR;
V_STEP_NAME        VARCHAR;
V_START_TIME       VARCHAR;
V_END_TIME         VARCHAR;

BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
   
V_STEP_NAME := ''COPY'';
   


IF (:FILE_SOURCE = ''ch_837p'' OR :FILE_SOURCE = ''ch_v837p'' OR :FILE_SOURCE = ''fek_837p'' OR :FILE_SOURCE = ''cms_837p'') THEN

V_PROF_QUERY := ''COPY INTO ''||:TGT_SC||''.''||''PROF_CLAIMS_RAW''||'' (XML, XML_MD5, FILE_NAME, FILE_SOURCE, ISDC_LOAD_DT) FROM (SELECT  $1 AS XML, MD5($1) AS XML_MD5, SUBSTR( METADATA$FILENAME, REGEXP_INSTR(METADATA$FILENAME,''''[/][^/]*$'''')+1) FILE_NAME, ''
||''''''''||:FILE_SOURCE||''''''''||'' AS FILE_SOURCE, CURRENT_TIMESTAMP::TIMESTAMP_NTZ(9) AS ISDC_LOAD_DT FROM ''||:STAGE_NAME||''/pre_clm/inbox/''||:FILE_SOURCE||''/data (FILE_FORMAT => ''||:FILE_FORMAT||'', PATTERN => ''''.*''||:FILE_PATTERN||''.*.txt'''')) ON_ERROR = CONTINUE ;''
;

V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP())
;
EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 
EXECUTE IMMEDIATE V_PROF_QUERY
;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID()) ;

V_LAST_QUERY_ID_CNT := (SELECT count(*) FROM TABLE ( RESULT_SCAN (:V_LAST_QUERY_ID)) WHERE $1 != ''Copy executed with 0 files processed.'');


                  IF (V_LAST_QUERY_ID_CNT != 0)
                  THEN 
                  V_ROWS_PARSED := (SELECT sum($3) FROM TABLE ( RESULT_SCAN (:V_LAST_QUERY_ID)));
                  V_ROWS_LOADED := (SELECT sum($4) FROM TABLE ( RESULT_SCAN (:V_LAST_QUERY_ID)));
                  V_MESSAGE := null;
                  END IF;
        


ELSEIF (:FILE_SOURCE = ''ch_837i'' OR :FILE_SOURCE = ''ch_v837i'' OR :FILE_SOURCE = ''fek_837i'') THEN

V_INST_QUERY := ''COPY INTO ''||:TGT_SC||''.''||''INST_CLAIMS_RAW''||'' (XML, XML_MD5, FILE_NAME, FILE_SOURCE, ISDC_LOAD_DT) FROM (SELECT  $1 AS XML, MD5($1) AS XML_MD5, SUBSTR( METADATA$FILENAME, REGEXP_INSTR(METADATA$FILENAME,''''[/][^/]*$'''')+1) FILE_NAME, ''
||''''''''||:FILE_SOURCE||''''''''||'' AS FILE_SOURCE, CURRENT_TIMESTAMP::TIMESTAMP_NTZ(9) AS ISDC_LOAD_DT FROM ''||:STAGE_NAME||''/pre_clm/inbox/''||:FILE_SOURCE||''/data (FILE_FORMAT => ''||:FILE_FORMAT||'', PATTERN => ''''.*''||:FILE_PATTERN||''.*.txt'''')) ON_ERROR = CONTINUE ;''
;

V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP())
;
EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 
EXECUTE IMMEDIATE V_INST_QUERY
;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID()) ;

V_LAST_QUERY_ID_CNT := (SELECT count(*) FROM TABLE ( RESULT_SCAN (:V_LAST_QUERY_ID)) WHERE $1 != ''Copy executed with 0 files processed.'');


              IF (V_LAST_QUERY_ID_CNT != 0)
              THEN 
              V_ROWS_PARSED := (SELECT sum($3) FROM TABLE ( RESULT_SCAN (:V_LAST_QUERY_ID)));
              V_ROWS_LOADED := (SELECT sum($4) FROM TABLE ( RESULT_SCAN (:V_LAST_QUERY_ID)));
              V_MESSAGE := null;
              END IF;
          


END IF;



   CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);


   EXCEPTION

   WHEN OTHER THEN

   CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


   RAISE;

   END;

';
