USE SCHEMA SRC_COMPAS;

CREATE OR REPLACE PROCEDURE "SP_SAVE_APP"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_COMPAS_SC" VARCHAR(16777216), "DELETE_APP_ID" VARCHAR(16777216), "SUBMIT_APP_ID" VARCHAR(16777216), "SAVE_APP_ID" VARCHAR(16777216), "RPT_START" NUMBER(38,0), "RPT_END" NUMBER(38,0), "APP_AGE_THRESHOLD" NUMBER(38,2), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER

AS '
DECLARE
	V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());
	V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
    V_MESSAGE                  VARCHAR;
    V_PROCESS_NAME             VARCHAR       DEFAULT ''SAVE_APP'';
	V_SUB_PROCESS_NAME		   VARCHAR		 DEFAULT ''SAVE_APP'';
	V_STEP					   VARCHAR;
	V_STEP_NAME        VARCHAR;
	V_START_TIME       VARCHAR;
	V_END_TIME         VARCHAR;
	V_ROWS_PARSED      INTEGER;
	V_ROWS_LOADED      INTEGER;
	V_LAST_QUERY_ID    VARCHAR;
    
    V_DAILY_APP_AGE_THRESHOLD  NUMBER(38,2);
    V_DISTINCT_COUNT INTEGER;
    V_TOTAL_COUNT INTEGER;
	

	V_TMP_APP_REPORT		   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.TMP_APP_REPORT'';
	V_TMP_INCOMPLETE_SAVE_COUNT    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.TMP_INCOMPLETE_SAVE_COUNT'';
    V_TMP_SUBMIT_COUNT    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.TMP_SUBMIT_COUNT'';

	V_STAGE_QUERY                 VARCHAR;
	V_CLG_MSG					    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_COMPAS_SC,''SRC_COMPAS'') || ''.CLG_MSG'';


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';

V_STEP_NAME := ''Load TMP_INCOMPLETE_SAVE_COUNT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_INCOMPLETE_SAVE_COUNT) AS 

select 
count(distinct concat_ws('';'',  (GET(XMLGET(XMLGET(XMLGET(saveapp.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR))) as app_count
from   IDENTIFIER(:V_CLG_MSG) as saveapp
left outer join (
          
          select concat_ws('';'',  (GET(XMLGET(XMLGET(XMLGET(submit_or_deleted_apps.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR))
          as OLE_ID
	      from   IDENTIFIER(:V_CLG_MSG) as submit_or_deleted_apps

	      where  submit_or_deleted_apps.msg_type_id in (:DELETE_APP_ID, :SUBMIT_APP_ID )
          	      and    concat_ws('';'',(GET(XMLGET(XMLGET(XMLGET(submit_or_deleted_apps.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR)) != ''''
                  and    submit_or_deleted_apps.msg_log_creation_date_dl between (:V_CURRENT_DATE - :RPT_START) and (:V_CURRENT_DATE - :RPT_END)

	      ) as submit_or_deleted_apps

     on (concat_ws('';'',(GET(XMLGET(XMLGET(XMLGET(saveapp.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR)) = submit_or_deleted_apps.OLE_ID)

     where  saveapp.msg_type_id = :SAVE_APP_ID
     and    saveapp.msg_status_id = ''2''
     and    saveapp.msg_log_creation_date_dl = (:V_CURRENT_DATE - :RPT_START)
     and    DATEDIFF( DAY, TO_DATE(saveapp.msg_log_creation_date_dl), (:V_CURRENT_DATE - :RPT_END) ) > :APP_AGE_THRESHOLD
     and    submit_or_deleted_apps.OLE_ID is null
     
     ;



V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_INCOMPLETE_SAVE_COUNT)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);  



V_STEP := ''STEP2'';

V_STEP_NAME := ''Load TMP_SUBMIT_COUNT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_SUBMIT_COUNT) AS 

     select     
     count(distinct concat_ws('';'',  (GET(XMLGET(XMLGET(XMLGET(submitapp.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR))) as app_count

     from   IDENTIFIER(:V_CLG_MSG) as submitapp

     where  submitapp.msg_type_id = :SUBMIT_APP_ID
	 and    concat_ws('';'',(GET(XMLGET(XMLGET(XMLGET(submitapp.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR)) != ''''
     and    submitapp.msg_log_creation_date_dl between  (:V_CURRENT_DATE - :RPT_START) and (:V_CURRENT_DATE - :RPT_END)

;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_SUBMIT_COUNT)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);  




V_DAILY_APP_AGE_THRESHOLD := (select round( (DIV0NULL(a.app_count , b.app_count) ) * 100, 2) from   IDENTIFIER(:V_TMP_INCOMPLETE_SAVE_COUNT) a , IDENTIFIER(:V_TMP_SUBMIT_COUNT) b);

IF (V_DAILY_APP_AGE_THRESHOLD > :APP_AGE_THRESHOLD) THEN 

V_STEP := ''STEP3'';

V_STEP_NAME := ''Load TMP_APP_REPORT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_APP_REPORT) AS

select distinct
	   saveapp.reference_id,
       concat_ws('';'',  (GET(XMLGET(XMLGET(XMLGET(saveapp.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR)) as OLE_ID,
	   saveapp.msg_log_creation_date::VARCHAR AS msg_log_creation_date

from   IDENTIFIER(:V_CLG_MSG) as saveapp
left outer join (
          
          select concat_ws('';'',  (GET(XMLGET(XMLGET(XMLGET(submit_or_deleted_apps.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR))
          as OLE_ID
	      from   IDENTIFIER(:V_CLG_MSG) as submit_or_deleted_apps

	      where  submit_or_deleted_apps.msg_type_id in (:DELETE_APP_ID, :SUBMIT_APP_ID )
          	      and    concat_ws('';'',(GET(XMLGET(XMLGET(XMLGET(submit_or_deleted_apps.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR)) != ''''
                  and    submit_or_deleted_apps.msg_log_creation_date_dl between (:V_CURRENT_DATE - :RPT_START) and (:V_CURRENT_DATE - :RPT_END)

	      ) as submit_or_deleted_apps

     on (concat_ws('';'',(GET(XMLGET(XMLGET(XMLGET(saveapp.msg_clob,''Body''),''SubmitApplicationRequest''),''OleReferenceIdentifier''),''$'')::VARCHAR)) = submit_or_deleted_apps.OLE_ID)

     where  saveapp.msg_type_id = :SAVE_APP_ID
     and    saveapp.msg_status_id = ''2''
     and    saveapp.msg_log_creation_date_dl = (:V_CURRENT_DATE - :RPT_START)
     and    DATEDIFF( DAY, TO_DATE(saveapp.msg_log_creation_date_dl), (:V_CURRENT_DATE - :RPT_END) ) > :APP_AGE_THRESHOLD
     and    submit_or_deleted_apps.OLE_ID is null
     
     ;
     


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_APP_REPORT)) ;
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);  
                                 


V_STEP := ''STEP4'';

V_STEP_NAME := ''Generate Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP()); 

V_DISTINCT_COUNT :=  (select app_count from IDENTIFIER(:V_TMP_INCOMPLETE_SAVE_COUNT));

V_TOTAL_COUNT := (select count(1) from IDENTIFIER(:V_TMP_APP_REPORT));

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/save_app/''||''saved_application_report_''||(select TO_VARCHAR(:V_CURRENT_DATE, ''YYYYMMDD''))||''.csv''||'' 

FROM 
(
  
select ''''There are ''||:V_DISTINCT_COUNT||'' distinct OLE IDs that have applications sitting in saved state'''', '''''''', ''''''''
UNION ALL
select ''''There are ''||:V_TOTAL_COUNT||'' total applications sitting in saved state'''','''''''', '''''''' 
UNION ALL
SELECT * FROM TMP_APP_REPORT 

)
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               empty_field_as_null=false
               NULL_IF = (''''NULL'''')
               compression = None
              )
               
               
OVERWRITE=True               
HEADER = False
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

execute immediate ''USE SCHEMA ''||:TGT_SC;                          
execute immediate :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);                        




                                 
END IF;
                                 
EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);



RAISE;

END;

';