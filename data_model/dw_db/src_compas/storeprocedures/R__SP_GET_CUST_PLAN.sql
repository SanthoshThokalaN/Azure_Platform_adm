
USE SCHEMA SRC_COMPAS;
CREATE OR REPLACE PROCEDURE SP_GET_CUST_PLAN("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_COMPAS_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "PREV_MONTH" VARCHAR(16777216), "THRESHOLD" VARCHAR(16777216), "GET_CUST_ID" VARCHAR(16777216), "SHOW_CUST_ID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

--V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

--Enter override for previous month in YYYY-MM format
V_PREVIOUS_MONTH VARCHAR:= COALESCE((:PREV_MONTH), TO_VARCHAR((CURRENT_DATE()-30)::date, ''yyyy-mm''));

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''GET_CUST_PLAN'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''GET_CUST_PLAN'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;

V_GET_CUST_TEMP           VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.GET_CUST_TEMP'';  
V_GET_CUSTOMER_CALLS      VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.GET_CUSTOMER_CALLS'';
V_GET_CUST_HEADER         VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.GET_CUST_HEADER'';
V_GET_CUST_REPORT_TEMP    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.GET_CUST_REPORT_TEMP'';  
    
    

V_CLG_MSG              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_COMPAS_SC,''SRC_COMPAS'') || ''.CLG_MSG''; 





BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
   
 
V_STEP_NAME := ''Create temporary table for reporting''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


    
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_GET_CUST_TEMP) AS 


SELECT TO_VARCHAR((XMLGET(XMLGET(XMLGET(XMLGET(sc.msg_clob, ''DataArea''),''Customer''),''Household''),''IndividualId''):"$")) as individual_id
, TO_VARCHAR(COUNT (DISTINCT (XMLGET(XMLGET(XMLGET(XMLGET(sc.msg_clob, ''DataArea''),''Customer''),''Household''),''IndividualId''):"$") || sc.msg_log_creation_date_dl)) AS number_of_calls
--,TO_VARCHAR(sc.msg_log_creation_date_dl::date, ''yyyy-mm'') AS CALL_DATE
FROM IDENTIFIER (:V_CLG_MSG) gc
JOIN IDENTIFIER (:V_CLG_MSG) sc
ON gc.reference_id = sc.reference_id
WHERE TO_VARCHAR(sc.msg_log_creation_date_dl::date, ''yyyy-mm'') = :V_PREVIOUS_MONTH
AND gc.msg_type_id = :GET_CUST_ID
AND sc.msg_type_id = :SHOW_CUST_ID
GROUP BY XMLGET(XMLGET(XMLGET(XMLGET(sc.msg_clob, ''DataArea''),''Customer''),''Household''),''IndividualId''):"$",
TO_VARCHAR(sc.msg_log_creation_date_dl::date, ''yyyy-mm'')
HAVING number_of_calls > :THRESHOLD

UNION

SELECT master.id_values as individual_id, TO_VARCHAR((COUNT(DISTINCT(master.val)))) AS number_of_calls
--,master.CALL_DATE AS CALL_DATE 
FROM
(
SELECT LISTAGG((XMLGET(final.xml,''IndividualId''):"$"),'';'') as id_values, concat_ws(LISTAGG((XMLGET(final.xml,''IndividualId''):"$"),'';''), final.dt) as val, TO_VARCHAR(final.dt::date, ''yyyy-mm'') AS CALL_DATE
FROM (
SELECT value as xml, SUBSTR(value, 2, regexp_instr(value, ''>'')-2) as segment, dt, key1 FROM
(
SELECT XMLGET(XMLGET(XMLGET(msg_clob, ''DataArea''),''Customer''),''Household'') as individual_id, msg_log_creation_date_dl AS dt, msg_log_id as key1 FROM IDENTIFIER (:V_CLG_MSG)
WHERE msg_type_id = :SHOW_CUST_ID
AND TO_VARCHAR(msg_log_creation_date_dl::date, ''yyyy-mm'') = :V_PREVIOUS_MONTH
) snip, lateral FLATTEN (snip.individual_id:"$")) final 
WHERE final.segment = ''Individual''
GROUP BY final.key1, final.dt) master
GROUP BY master.id_values, master.CALL_DATE
HAVING COUNT(DISTINCT(master.val)) > :THRESHOLD;


    

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_GET_CUST_TEMP)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


   V_STEP := ''STEP2'';
   
   V_STEP_NAME := ''Insert partitioned report data for archival''; 
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

INSERT INTO IDENTIFIER(:V_GET_CUSTOMER_CALLS)

SELECT TO_VARCHAR((XMLGET(XMLGET(XMLGET(XMLGET(sc.msg_clob, ''DataArea''),''Customer''),''Household''),''IndividualId''):"$")) as INDIVIDUAL_ID
, TO_VARCHAR(COUNT (DISTINCT (XMLGET(XMLGET(XMLGET(XMLGET(sc.msg_clob, ''DataArea''),''Customer''),''Household''),''IndividualId''):"$") || sc.msg_log_creation_date_dl)) AS NUMBER_OF_CALLS,
TO_VARCHAR(sc.msg_log_creation_date_dl::date, ''yyyy-mm'') AS CALL_DATE,
CURRENT_TIMESTAMP() , CURRENT_TIMESTAMP()
FROM IDENTIFIER (:V_CLG_MSG) gc
JOIN IDENTIFIER (:V_CLG_MSG) sc
ON gc.reference_id = sc.reference_id
WHERE TO_VARCHAR(sc.msg_log_creation_date_dl::date, ''yyyy-mm'') = :V_PREVIOUS_MONTH
AND gc.msg_type_id = :GET_CUST_ID
GROUP BY XMLGET(XMLGET(XMLGET(XMLGET(sc.msg_clob, ''DataArea''),''Customer''),''Household''),''IndividualId''):"$",
TO_VARCHAR(sc.msg_log_creation_date_dl::date, ''yyyy-mm'')
HAVING number_of_calls > :THRESHOLD

UNION

SELECT master.id_values as INDIVIDUAL_ID, TO_VARCHAR((COUNT(DISTINCT(master.val)))) AS NUMBER_OF_CALLS, master.CALL_DATE AS CALL_DATE, CURRENT_TIMESTAMP() , CURRENT_TIMESTAMP() FROM
(
SELECT LISTAGG((XMLGET(final.xml,''IndividualId''):"$"),'';'') as id_values, concat_ws(LISTAGG((XMLGET(final.xml,''IndividualId''):"$"),'';''), final.dt) as val, TO_VARCHAR(final.dt::date, ''yyyy-mm'') AS CALL_DATE
FROM (
SELECT value as xml, SUBSTR(value, 2, regexp_instr(value, ''>'')-2) as segment, dt, key1 FROM
(
SELECT XMLGET(XMLGET(XMLGET(msg_clob, ''DataArea''),''Customer''),''Household'') as individual_id, msg_log_creation_date_dl AS dt, msg_log_id as key1 FROM IDENTIFIER (:V_CLG_MSG)
WHERE msg_type_id = :SHOW_CUST_ID
AND TO_VARCHAR(msg_log_creation_date_dl::date, ''yyyy-mm'') = :V_PREVIOUS_MONTH
) snip, lateral FLATTEN (snip.individual_id:"$")) final 
WHERE final.segment = ''Individual''
GROUP BY final.key1, final.dt) master
GROUP BY master.id_values, master.CALL_DATE
HAVING COUNT(DISTINCT(master.val)) > :THRESHOLD;   


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_GET_CUSTOMER_CALLS) where CALL_DATE=:V_PREVIOUS_MONTH) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);





V_STEP := ''STEP3'';
   
V_STEP_NAME := ''Create final report''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

CREATE TEMPORARY TABLE IDENTIFIER(:V_GET_CUST_REPORT_TEMP)
(
Individual_ID VARCHAR(16777216),
NUMBER_OF_CALLS VARCHAR(16777216)
);

INSERT INTO IDENTIFIER(:V_GET_CUST_REPORT_TEMP) 
VALUES (''Individual ID(s)'', ''Count of Calls'')
;

INSERT INTO IDENTIFIER(:V_GET_CUST_REPORT_TEMP)
SELECT * FROM IDENTIFIER(:V_GET_CUST_TEMP);


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_GET_CUST_REPORT_TEMP)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);





                                 
                                 
V_STEP := ''STEP4'';

 
V_STEP_NAME := ''Generate report for get_cust_plan''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/get_cust_plan/''||''get_customer_report_''||:V_PREVIOUS_MONTH||''.csv''||'' FROM (
             SELECT *
               FROM SRC_COMPAS.GET_CUST_REPORT_TEMP
               ORDER BY Individual_id DESC
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               compression = None             
               )
               
               
               
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

--execute immediate ''USE SCHEMA ''||:UTIL_SC;                          
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