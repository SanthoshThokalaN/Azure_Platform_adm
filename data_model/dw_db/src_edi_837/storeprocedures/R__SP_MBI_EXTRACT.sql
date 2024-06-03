
USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE "SP_MBI_EXTRACT"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "LZ_EDI_837_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '

DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME             VARCHAR        default ''MBI_EXTRACT'';
V_SUB_PROCESS_NAME         VARCHAR        default ''MBI_EXTRACT'';
V_STEP                     VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER; 
V_ROWS_LOADED              INTEGER; 
V_MESSAGE                  VARCHAR;
V_LAST_QUERY_ID            VARCHAR; 
V_STAGE_QUERY              VARCHAR; 




V_TMP_MEMBER                     VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_MEMBER'';
V_TMP_MBI_STG                    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_MBI_STG'';
V_TMP_MBI_STG_1                  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_MBI_STG_1'';
V_TMP_MBI_TARGET_1               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_MBI_TARGET_1'';
V_TMP_MBI_TARGET                 VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_MBI_TARGET'';
    
V_MEMBER_STG               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_EDI_837_SC,''LZ_EDI_837_SC'') || ''.MEMBER_STG'';
V_INST_SUBSCRIBER          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'')  || ''.INST_SUBSCRIBER'';
V_PROF_SUBSCRIBER          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'')  || ''.PROF_SUBSCRIBER'';

    

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
 
 
V_STEP_NAME := ''Load TMP_MEMBER''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_MEMBER)  AS 
  
SELECT  
SUBSTR(SPLIT_PART(regexp_replace(replace(member, '' '', ''~''), ''\\\\~+'', ''~''), ''~'', 1), 13)  AS subscriber_name_last ,
SPLIT_PART(regexp_replace(replace(member, '' '', ''~''), ''\\\\~+'', ''~''), ''~'', 2) AS subscriber_name_first,
SUBSTR(SPLIT_PART(regexp_replace(replace(member, '' '', ''~''), ''\\\\~+'', ''~''), ''~'', 3), 1, length(SPLIT_PART(regexp_replace(replace(member, '' '', ''~''), ''\\\\~+'', ''~''), ''~'', 3))-1) AS subscriber_dateofbirth,
SUBSTR(SPLIT_PART(regexp_replace(replace(member, '' '', ''~''), ''\\\\~+'', ''~''), ''~'', 3),-1, 1) as subscriber_gendercode,
SPLIT_PART(regexp_replace(replace(member, '' '', ''~''), ''\\\\~+'', ''~''), ''~'', 4) as member,
SPLIT_PART(regexp_replace(replace(member, '' '', ''~''), ''\\\\~+'', ''~''), ''~'', 5) as  type
from IDENTIFIER(:V_MEMBER_STG)
;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_MEMBER)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
V_STEP := ''STEP2'';
 
 
V_STEP_NAME := ''Load TMP_MBI_STG''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_MBI_STG)  AS 

SELECT 
B.MEMBER AS SUBSCRIBER_ID,
A.OTHER_SUBSCRIBER_ID,
TRIM(B.SUBSCRIBER_NAME_FIRST) AS SUBSCRIBER_NAME_FIRST, 
TRIM(B.SUBSCRIBER_NAME_LAST) AS SUBSCRIBER_NAME_LAST,
A.TRANSACTSET_CREATE_DATE, 
B.SUBSCRIBER_DATEOFBIRTH,  
B.SUBSCRIBER_GENDERCODE 
FROM IDENTIFIER(:V_INST_SUBSCRIBER)  A 
JOIN IDENTIFIER(:V_TMP_MEMBER) B ON  (A.SUBSCRIBER_ID = B.MEMBER) 
WHERE A.APP_SENDER_CODE = ''APTIX'' AND A.TRANSACTSET_CREATE_DATE >= ''20180101'' 
AND LENGTH(A.OTHER_SUBSCRIBER_ID) = ''11'' 
AND SUBSTR(OTHER_SUBSCRIBER_ID,1,1) RLIKE ''^[1-9]+'' 
AND B.TYPE = ''BO01'' 
AND B.MEMBER != A.OTHER_SUBSCRIBER_ID


UNION

SELECT 
B.MEMBER AS SUBSCRIBER_ID,
A.OTHER_SUBSCRIBER_ID,
TRIM(B.SUBSCRIBER_NAME_FIRST) AS SUBSCRIBER_NAME_FIRST,
TRIM(B.SUBSCRIBER_NAME_LAST) AS SUBSCRIBER_NAME_LAST,
A.TRANSACTSET_CREATE_DATE,
B.SUBSCRIBER_DATEOFBIRTH,
B.SUBSCRIBER_GENDERCODE 
FROM IDENTIFIER(:V_PROF_SUBSCRIBER) A JOIN IDENTIFIER(:V_TMP_MEMBER) B ON (A.SUBSCRIBER_ID = B.MEMBER) 
WHERE A.APP_SENDER_CODE = ''APTIX'' AND A.TRANSACTSET_CREATE_DATE >= ''20180101'' 
AND LENGTH(A.OTHER_SUBSCRIBER_ID) = ''11'' 
AND SUBSTR(OTHER_SUBSCRIBER_ID,1,1) RLIKE ''^[1-9]+'' 
AND B.TYPE = ''BO01'' 
AND B.MEMBER != A.OTHER_SUBSCRIBER_ID

;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_MBI_STG)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
V_STEP := ''STEP3'';

V_STEP_NAME := ''Load TMP_MBI_STG_1''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                   

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_MBI_STG_1) AS
SELECT 
SUBSCRIBER_ID,
OTHER_SUBSCRIBER_ID,
SUBSCRIBER_NAME_LAST,
SUBSCRIBER_NAME_FIRST,
TRANSACTSET_CREATE_DATE,
SUBSCRIBER_DATEOFBIRTH,
SUBSCRIBER_GENDERCODE 
FROM 
(
SELECT 
SUBSCRIBER_ID,
OTHER_SUBSCRIBER_ID,
SUBSCRIBER_NAME_LAST,
SUBSCRIBER_NAME_FIRST,
TRANSACTSET_CREATE_DATE,
SUBSCRIBER_DATEOFBIRTH,
SUBSCRIBER_GENDERCODE,
MAX(TRANSACTSET_CREATE_DATE) OVER (PARTITION BY SUBSCRIBER_ID) AS LAST_MODIFIED 
FROM IDENTIFIER(:V_TMP_MBI_STG)
) AS SUB 
WHERE TRANSACTSET_CREATE_DATE = LAST_MODIFIED
       
;
  
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_MBI_STG_1)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
  
V_STEP := ''STEP4'';
 
V_STEP_NAME := ''Load TMP_MBI_TARGET_1''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());          

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_MBI_TARGET_1) AS 
SELECT 
SUBSCRIBER_ID,
OTHER_SUBSCRIBER_ID,
SUBSCRIBER_NAME_LAST,
SUBSCRIBER_NAME_FIRST,
TRANSACTSET_CREATE_DATE,
SUBSCRIBER_DATEOFBIRTH,
SUBSCRIBER_GENDERCODE 
FROM IDENTIFIER(:V_TMP_MBI_STG)
WHERE 
SUBSCRIBER_ID IN 
(
SELECT SUBSCRIBER_ID 
FROM (SELECT M.*,COUNT(CONCAT(SUBSCRIBER_ID,OTHER_SUBSCRIBER_ID,TRANSACTSET_CREATE_DATE)) 
OVER (PARTITION BY SUBSCRIBER_NAME_FIRST,SUBSCRIBER_NAME_LAST,TRANSACTSET_CREATE_DATE,SUBSCRIBER_DATEOFBIRTH,SUBSCRIBER_GENDERCODE) AS RN FROM IDENTIFIER(:V_TMP_MBI_STG_1) M) M2 
WHERE M2.RN = ''1''
);
  
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_MBI_TARGET_1)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);  

V_STEP := ''STEP5'';
 
V_STEP_NAME := ''Load TMP_MBI_TARGET''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());         
  
  
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_MBI_TARGET)  AS 
SELECT 
SUBSCRIBER_ID,
OTHER_SUBSCRIBER_ID,
SUBSCRIBER_NAME_LAST,
SUBSCRIBER_NAME_FIRST,
TRANSACTSET_CREATE_DATE,
SUBSCRIBER_DATEOFBIRTH,
SUBSCRIBER_GENDERCODE
FROM 
(SELECT M.*, 
  ROW_NUMBER() OVER (PARTITION BY SUBSCRIBER_ID ORDER BY TRANSACTSET_CREATE_DATE DESC) AS RN FROM IDENTIFIER(:V_TMP_MBI_TARGET_1) M
) M2 
WHERE M2.RN = ''1''
;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_MBI_TARGET)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
                           
                                 

V_STEP := ''STEP6'';

 
V_STEP_NAME := ''Generate Report for MBI Extract''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/mbi_extract/''||''MBIs_Found_837_''||(select TO_VARCHAR(current_date(),''YYYYMMDD''))||''.txt''||'' FROM (
             SELECT *
               FROM TMP_MBI_TARGET
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''|''''
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
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);                                      
                                 

EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';