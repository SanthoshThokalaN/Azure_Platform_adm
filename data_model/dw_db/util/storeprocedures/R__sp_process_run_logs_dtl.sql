USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE "SP_PROCESS_RUN_LOGS_DTL"("LOG_DB" VARCHAR(16777216), "LOG_SC" VARCHAR(16777216), "P_APP_NAME" VARCHAR(16777216), "P_PIPELINE_ID" VARCHAR(16777216), 
                                                      "P_PIPELINE_NAME" VARCHAR(16777216), "P_PROCESS_NAME" VARCHAR(16777216), "P_SUB_PROCESS_NAME" VARCHAR(16777216), 
                                                      "P_STEP" VARCHAR(16777216), "P_STEP_NAME" VARCHAR(16777216), "P_START_DATETIME" VARCHAR(16777216), 
                                                      "P_END_DATETIME" VARCHAR(16777216), "P_STATUS" VARCHAR(16777216), "P_LAST_QUERY_ID" VARCHAR(16777216), 
                                                      "P_ROWS_PARSED" VARCHAR(16777216), "P_ROWS_LOADED" VARCHAR(16777216), "P_MESSAGE" VARCHAR(16777216), 
                                                      "P_ERROR_CODE" VARCHAR(16777216), "P_ERROR_STATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

v_PROCESS_RUN_LOGS_DTL  varchar := :LOG_DB||''.''||coalesce(:LOG_SC, ''UTIL'')||''.PROCESS_RUN_LOGS_DTL'';

BEGIN

   insert into identifier(:v_PROCESS_RUN_LOGS_DTL) 
   (

    PIPELINE_ID,
	PIPELINE_NAME ,
    APP_NAME,
	PROCESS_NAME ,
	SUB_PROCESS_NAME ,
	STEP      ,
	STEP_NAME ,
	START_DATETIME ,
	END_DATETIME ,
	STATUS ,
    LAST_QUERY_ID,
    ROWS_PARSED,
    ROWS_LOADED,
	MESSAGE ,
	ERROR_CODE ,
	ERROR_STATE 
    ) 
    values 
   ( :P_PIPELINE_ID, 
     :P_PIPELINE_NAME, 
     :P_APP_NAME,
     :P_PROCESS_NAME, 
     :P_SUB_PROCESS_NAME, 
     :P_STEP, 
     :P_STEP_NAME, 
     :P_START_DATETIME, 
     :P_END_DATETIME, 
     :P_STATUS, 
     :P_LAST_QUERY_ID,
     :P_ROWS_PARSED,
     :P_ROWS_LOADED,
     :P_MESSAGE, 
     :P_ERROR_CODE, 
     :P_ERROR_STATE
    );
    
   return ''success'';
   
   EXCEPTION 
   
   WHEN OTHER THEN 

   raise;
   
   end;
';

