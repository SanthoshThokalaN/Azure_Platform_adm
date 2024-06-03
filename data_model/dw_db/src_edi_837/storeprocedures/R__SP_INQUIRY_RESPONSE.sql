USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_INQUIRY_RESPONSE("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_COMPAS_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE



V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME      VARCHAR DEFAULT ''Inquiry Response'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT ''Inquiry Response Process'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

 
V_CLG_MSG            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_COMPAS_SC,''SRC_COMPAS'') || ''.CLG_MSG''; 
V_TMP_INQUIRY_RESPONSE_REPORT VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_INQUIRY_RESPONSE_REPORT'';
V_INQUIRY_COUNTER 	VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.INQUIRY_COUNTER'';
V_MSG_LOG_CREATION_DATE varchar;
V_STAGE_QUERY1  VARCHAR;
V_STAGE_QUERY2  VARCHAR;
V_STAGE_QUERY3  VARCHAR;
V_STAGE_QUERY4  VARCHAR;
V_STAGE_QUERY5  VARCHAR;
V_STAGE_QUERY6  VARCHAR;
V_STAGE_QUERY7  VARCHAR;
V_RESULT varchar;
V_INSERT_QUERY varchar;
V_10AM_TRIGGER VARCHAR;
V_12PM_TRIGGER VARCHAR;
V_14PM_TRIGGER VARCHAR;
V_16PM_TRIGGER VARCHAR;

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';

V_STEP_NAME := ''GENERATE REPORT FOR INQUIRY_RESPONSE1''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

select count(1) into :V_RESULT from IDENTIFIER(:V_INQUIRY_COUNTER) where run_date=to_char(current_date,''YYYY-MM-DD'');

V_INSERT_QUERY := ''insert into SRC_EDI_837.INQUIRY_COUNTER (Run_Hours,Ind_flg,Run_date) VALUES (''''10'''',''''N'''',current_date),
(''''12'''',''''N'''',current_date),(''''14'''',''''N'''',current_date),(''''16'''',''''N'''',current_date)'';

IF (V_RESULT = 0) THEN

execute immediate ''USE SCHEMA ''||:TGT_SC;  
execute immediate  :V_INSERT_QUERY;  

END IF;

select IND_FLG into :V_10AM_TRIGGER from IDENTIFIER(:V_INQUIRY_COUNTER) where run_date=to_char(current_date,''YYYY-MM-DD'') AND RUN_HOURS=10;
select IND_FLG into :V_12PM_TRIGGER from IDENTIFIER(:V_INQUIRY_COUNTER) where run_date=to_char(current_date,''YYYY-MM-DD'') AND RUN_HOURS=12;
select IND_FLG into :V_14PM_TRIGGER from IDENTIFIER(:V_INQUIRY_COUNTER) where run_date=to_char(current_date,''YYYY-MM-DD'') AND RUN_HOURS=14;
select IND_FLG into :V_16PM_TRIGGER from IDENTIFIER(:V_INQUIRY_COUNTER) where run_date=to_char(current_date,''YYYY-MM-DD'') AND RUN_HOURS=16;

SELECT substr(to_varchar(MAX(msg_log_creation_date)::timestamp,''YYYY-MM-DD HH24:MI:SS.FF''),12,2) as HOUR INTO :V_MSG_LOG_CREATION_DATE
FROM IDENTIFIER(:V_CLG_MSG)
WHERE msg_log_creation_date_dl = (current_date)
 AND msg_type_id = ''101018'';



V_STAGE_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE  TMP_PREVIOUS_WINDOW AS 
SELECT IFNULL(NULLIFZERO(COUNT(msg_log_id)),1) as PREVIOUSWINDOW
FROM SRC_COMPAS.CLG_MSG WHERE msg_log_creation_date_dl = current_date()-7
AND msg_type_id = ''''101018''''
AND msg_log_creation_date BETWEEN to_varchar(dateadd(hour,-2,dateadd(day,-7,current_timestamp())),''''YYYY-MM-DD HH24:00:00'''') AND to_varchar(dateadd(day,-7,current_timestamp()),''''YYYY-MM-DD HH24:00:00'''');'';



V_STAGE_QUERY2 := ''CREATE OR REPLACE TEMPORARY TABLE  TMP_CURRENT_WINDOW AS 
SELECT COUNT(msg_log_id) as CURRENTWINDOW
FROM SRC_COMPAS.CLG_MSG WHERE msg_log_creation_date_dl = current_date()
AND msg_type_id = ''''101018''''
AND msg_log_creation_date BETWEEN to_varchar(dateadd(hour,-2,current_timestamp()),''''YYYY-MM-DD HH24:00:00'''') AND to_varchar(current_timestamp(),''''YYYY-MM-DD HH24:00:00'''');'';

V_STAGE_QUERY3 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/inquiry_response/''||''report_''||(select TO_VARCHAR(DATEADD(HOUR,-1,CURRENT_TIMESTAMP),''YYYY-MM-DD_HH240000''))||''.txt''||'' FROM (
                          
          select  concat(''''Window Start Time ='''',(Select TO_VARCHAR(DATEADD(HOUR,-2,CURRENT_TIMESTAMP),''''YYYY-MM-DD_HH24:00:00'''')))
          union 
          select  concat(''''Window End Time ='''',(Select TO_VARCHAR(CURRENT_TIMESTAMP,''''YYYY-MM-DD_HH24:00:00'''')))
          union
          select  Concat(concat(''''Expected Count :'''',(SELECT PREVIOUSWINDOW FROM TMP_PREVIOUS_WINDOW)),
                         concat('''', Current Window Count :'''',(SELECT CURRENTWINDOW FROM TMP_CURRENT_WINDOW)))
          union
          select concat(''''Variance ='''',(SELECT (((CURRENTWINDOW - PREVIOUSWINDOW)*100)/PREVIOUSWINDOW)::Number(38,4) as variance FROM TMP_PREVIOUS_WINDOW , TMP_CURRENT_WINDOW))
          union 
          select concat(''''Percentage Difference ='''',(select ((CURRENTWINDOW*100)/PREVIOUSWINDOW)::Number(38,4) as percentageDifference FROM TMP_PREVIOUS_WINDOW , TMP_CURRENT_WINDOW))
               
)
file_format = (type = ''''CSV''''
               field_delimiter = None
               compression = None
               )
                      
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True'';

V_STAGE_QUERY4 := ''Update INQUIRY_COUNTER set IND_FLG=''''Y'''' where run_date=to_char(current_date,''''YYYY-MM-DD'''') AND RUN_HOURS=10'';
V_STAGE_QUERY5 := ''Update INQUIRY_COUNTER set IND_FLG=''''Y'''' where run_date=to_char(current_date,''''YYYY-MM-DD'''') AND RUN_HOURS=12'';
V_STAGE_QUERY6 := ''Update INQUIRY_COUNTER set IND_FLG=''''Y'''' where run_date=to_char(current_date,''''YYYY-MM-DD'''') AND RUN_HOURS=14'';
V_STAGE_QUERY7 := ''Update INQUIRY_COUNTER set IND_FLG=''''Y'''' where run_date=to_char(current_date,''''YYYY-MM-DD'''') AND RUN_HOURS=16'';
 
IF (V_MSG_LOG_CREATION_DATE = 10) THEN 

IF (V_10AM_TRIGGER = ''N'') THEN

execute immediate ''USE SCHEMA ''||:TGT_SC;                                
execute immediate  :V_STAGE_QUERY1;  
execute immediate  :V_STAGE_QUERY2;
execute immediate  :V_STAGE_QUERY3;
execute immediate  :V_STAGE_QUERY4;
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1));

END IF;

ELSEIF(V_MSG_LOG_CREATION_DATE = 12) THEN 

IF (V_12PM_TRIGGER = ''N'') THEN

execute immediate ''USE SCHEMA ''||:TGT_SC;                                
execute immediate  :V_STAGE_QUERY1;  
execute immediate  :V_STAGE_QUERY2;
execute immediate  :V_STAGE_QUERY3;
execute immediate  :V_STAGE_QUERY5;
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1));

END IF;

ELSEIF(V_MSG_LOG_CREATION_DATE = 14) THEN 

IF (V_14PM_TRIGGER = ''N'') THEN

execute immediate ''USE SCHEMA ''||:TGT_SC;                                
execute immediate  :V_STAGE_QUERY1;  
execute immediate  :V_STAGE_QUERY2;
execute immediate  :V_STAGE_QUERY3;
execute immediate  :V_STAGE_QUERY6;
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1));

END IF;

ELSEIF(V_MSG_LOG_CREATION_DATE = 16) THEN 

IF (V_16PM_TRIGGER = ''N'') THEN

execute immediate ''USE SCHEMA ''||:TGT_SC;                                
execute immediate  :V_STAGE_QUERY1;  
execute immediate  :V_STAGE_QUERY2;
execute immediate  :V_STAGE_QUERY3;
execute immediate  :V_STAGE_QUERY7;
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1));

END IF;

V_ROWS_LOADED := (select count(1) from TMP_PREVIOUS_WINDOW) ;

END IF; 





CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   
                                 

 	 
EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);



RAISE;

END;

';