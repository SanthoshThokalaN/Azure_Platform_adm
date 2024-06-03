USE SCHEMA SRC_COMPAS;

CREATE OR REPLACE PROCEDURE "SP_EFFECTIVE_ERROR_REPORT"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "LZ_COMPAS_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());
V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_MESSAGE                  VARCHAR;
V_PROCESS_NAME             VARCHAR       DEFAULT ''EFFECTIVE_ERROR'';
V_SUB_PROCESS_NAME		   VARCHAR		 DEFAULT ''EFFECTIVE_ERROR_REPORT'';
V_STEP					   VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER;
V_ROWS_LOADED              INTEGER;
V_LAST_QUERY_ID            VARCHAR;
V_STAGE_QUERY              VARCHAR;
  
  
V_TMP_EFFECTIVE_ERROR_REPORT	        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.TMP_EFFECTIVE_ERROR_REPORT'';
  
  
V_EFFECTIVE_ERROR_REPORT_STG	VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.EFFECTIVE_ERROR_REPORT_STG'';
V_EFFECTIVE_ERROR_MEMBERSHIP_INFO_STG	VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.EFFECTIVE_ERROR_MEMBERSHIP_INFO_STG'';
  


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';

V_STEP_NAME := ''Load TMP_EFFECTIVE_ERROR_MONTHLY_REPORT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
    
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_EFFECTIVE_ERROR_REPORT) AS 
(
  
select
Failure_Type,
concat(membership_number, ''-'', association_id, insured_cd) as Membership_Number,
Routing_Number,
Account_Number,
Address_Type,
Address_Line_1,
Address_Line_2,
City,
State_CD,
Zip_CD,
Country
from
IDENTIFIER(:V_EFFECTIVE_ERROR_REPORT_STG) eer_stg
join IDENTIFIER(:V_EFFECTIVE_ERROR_MEMBERSHIP_INFO_STG) eermi_stg 
on eer_stg.individual_id = eermi_stg.individual_id

)
;

 
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_EFFECTIVE_ERROR_REPORT)) ; 

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
 
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

  
  
V_STEP := ''STEP2'';

V_STEP_NAME := ''Generate Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/effective_error/''||
((SELECT CASE WHEN DAY(:V_CURRENT_DATE) = 1 THEN ''Failed_Transaction_Retro_Date_Monthly_'' ELSE ''Failed_Transaction_Retro_Date_'' END) 
||(select TO_VARCHAR(:V_CURRENT_DATE, ''MMDDYYYY'')))||''.csv''||'' 



FROM (
             
     select ''''Failure_Type'''',''''Membership_Number'''',''''Routing_Number'''',''''Account_Number'''',''''Address_Type'''',''''Address_Line_1'''',''''Address_Line_2'''',''''City'''',''''State_CD'''',''''Zip_CD'''',''''Country''''
     UNION 
             SELECT Failure_Type,Membership_Number,Routing_Number,Account_Number,Address_Type,Address_Line_1,Address_Line_2,City,State_CD,Zip_CD,Country
               FROM TMP_EFFECTIVE_ERROR_REPORT
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               empty_field_as_null=false
               NULL_IF = (''''NULL'''')
               compression = None
              )
               
               
               
HEADER = False
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
