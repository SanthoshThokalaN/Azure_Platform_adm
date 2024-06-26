USE SCHEMA SRC_COMPAS;

CREATE OR REPLACE PROCEDURE "SP_UPDATE_PAYMENT_RESPONSE"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "LZ_COMPAS_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "TIME_OUT_IN_MINUTES" NUMBER(38,0), "TIME_COLUMN_TO_CONSIDER" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''SP_UPDATE_PAYMENT_RESPONSE'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''SP_UPDATE_PAYMENT_RESPONSE'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;
  

V_XML_CLEANED_STG                   VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.XML_CLEANED_STG'';
V_XML_FAILED_STG                    VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.XML_FAILED_STG'';    
V_CMP_UPDATEPAYMENT_ERROREXCLUSION  VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.CMP_UPDATEPAYMENT_ERROREXCLUSION'';
V_REQ_RES_PAIR                      VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.REQ_RES_PAIR''; 

V_REQUEST                           VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.REQUEST''; 
V_RESPONSE                          VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.RESPONSE'';
V_SUCCESSFUL_REQUEST                VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.SUCCESSFUL_REQUEST''; 
V_SUCCESSFUL_RESPONSE               VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.SUCCESSFUL_RESPONSE'';
V_FAIL_REQUEST                      VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.FAIL_REQUEST''; 
V_FAIL_RESPONSE                     VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.FAIL_RESPONSE'';
V_REQUEST_BUFFER                    VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.REQUEST_BUFFER'';
V_CTE                               VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.CTE'';

V_COMPAS_REPORTING                  VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.COMPAS_REPORTING'';
V_OUR_LOG                           VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.OUR_LOG'';


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';


V_STEP := ''STEP1'';
   
V_STEP_NAME := ''Create all tables used for the process''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());



CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_REQUEST) AS 
SELECT 
    MSG_TYPE_ID,
    MSG_DESC,
	INBOUND,
	MSG_CATEGORY_ID,
    MSG_TYPE_CREATED_BY,
	MSG_TYPE_CREATION_DATE,
	MSG_TYPE_LAST_MODIFIED_BY,
	MSG_TYPE_LAST_MODIFIED_DATE,
	MSG_LOG_ID,
	BODID,
	MSG_STATUS_ID,
	REFERENCE_ID,
	MSG_LOG_CREATE_DATE,
	MSG_LOG_CREATED_BY,
	MSG_LOG_CREATION_DATE,
	MSG_TEXT_ID,
	MSG_CLOB FROM IDENTIFIER (:V_XML_CLEANED_STG) t1
WHERE MSG_TYPE_ID IN (SELECT Request FROM IDENTIFIER (:V_REQ_RES_PAIR))
AND MSG_LOG_CREATION_DATE = (
SELECT MIN(t2.MSG_LOG_CREATION_DATE) FROM IDENTIFIER (:V_XML_CLEANED_STG) t2
WHERE t2.MSG_TYPE_ID = t1.MSG_TYPE_ID
AND
t2.REFERENCE_ID = t1.REFERENCE_ID
);

INSERT INTO  IDENTIFIER (:V_REQUEST)
SELECT MSG_TYPE_ID,
    MSG_DESC,
	INBOUND,
	MSG_CATEGORY_ID,
    MSG_TYPE_CREATED_BY,
	MSG_TYPE_CREATION_DATE,
	MSG_TYPE_LAST_MODIFIED_BY,
	MSG_TYPE_LAST_MODIFIED_DATE,
	MSG_LOG_ID,
	BODID,
	MSG_STATUS_ID,
	REFERENCE_ID,
	MSG_LOG_CREATE_DATE,
	MSG_LOG_CREATED_BY,
	MSG_LOG_CREATION_DATE,
	MSG_TEXT_ID,
	MSG_CLOB FROM IDENTIFIER (:V_REQUEST_BUFFER);


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_RESPONSE) AS
SELECT 
    MSG_TYPE_ID,
    MSG_DESC,
	INBOUND,
	MSG_CATEGORY_ID,
    MSG_TYPE_CREATED_BY,
	MSG_TYPE_CREATION_DATE,
	MSG_TYPE_LAST_MODIFIED_BY,
	MSG_TYPE_LAST_MODIFIED_DATE,
	MSG_LOG_ID,
	BODID,
	MSG_STATUS_ID,
	REFERENCE_ID,
	MSG_LOG_CREATE_DATE,
	MSG_LOG_CREATED_BY,
	MSG_LOG_CREATION_DATE,
	MSG_TEXT_ID,
	MSG_CLOB FROM IDENTIFIER (:V_XML_CLEANED_STG) t1
WHERE MSG_TYPE_ID IN (SELECT Response FROM IDENTIFIER (:V_REQ_RES_PAIR))
AND MSG_LOG_CREATION_DATE = (
SELECT MIN(MSG_LOG_CREATION_DATE) FROM IDENTIFIER (:V_XML_CLEANED_STG) t2
WHERE t2.MSG_TYPE_ID = t1.MSG_TYPE_ID
AND
t2.REFERENCE_ID = t1.REFERENCE_ID
);



CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_FAIL_REQUEST) AS
SELECT MSG_TYPE_ID,
    MSG_DESC,
	INBOUND,
	MSG_CATEGORY_ID,
    MSG_TYPE_CREATED_BY,
	MSG_TYPE_CREATION_DATE,
	MSG_TYPE_LAST_MODIFIED_BY,
	MSG_TYPE_LAST_MODIFIED_DATE,
	MSG_LOG_ID,
	BODID,
	MSG_STATUS_ID,
	REFERENCE_ID,
	MSG_LOG_CREATE_DATE,
	MSG_LOG_CREATED_BY,
	MSG_LOG_CREATION_DATE,
	MSG_TEXT_ID,
	MSG_CLOB FROM IDENTIFIER (:V_REQUEST)
WHERE msg_status_id != 4;

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_SUCCESSFUL_REQUEST) AS
SELECT MSG_TYPE_ID,
    MSG_DESC,
	INBOUND,
	MSG_CATEGORY_ID,
    MSG_TYPE_CREATED_BY,
	MSG_TYPE_CREATION_DATE,
	MSG_TYPE_LAST_MODIFIED_BY,
	MSG_TYPE_LAST_MODIFIED_DATE,
	MSG_LOG_ID,
	BODID,
	MSG_STATUS_ID,
	REFERENCE_ID,
	MSG_LOG_CREATE_DATE,
	MSG_LOG_CREATED_BY,
	MSG_LOG_CREATION_DATE,
	MSG_TEXT_ID,
	MSG_CLOB FROM IDENTIFIER (:V_REQUEST)
WHERE msg_status_id = 4;






CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_FAIL_RESPONSE) AS
SELECT MSG_TYPE_ID,
    MSG_DESC,
	INBOUND,
	MSG_CATEGORY_ID,
    MSG_TYPE_CREATED_BY,
	MSG_TYPE_CREATION_DATE,
	MSG_TYPE_LAST_MODIFIED_BY,
	MSG_TYPE_LAST_MODIFIED_DATE,
	MSG_LOG_ID,
	BODID,
	MSG_STATUS_ID,
	REFERENCE_ID,
	MSG_LOG_CREATE_DATE,
	MSG_LOG_CREATED_BY,
	MSG_LOG_CREATION_DATE,
	MSG_TEXT_ID,
	MSG_CLOB FROM IDENTIFIER (:V_RESPONSE)
WHERE msg_status_id = 1;

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_SUCCESSFUL_RESPONSE) AS
SELECT MSG_TYPE_ID,
    MSG_DESC,
	INBOUND,
	MSG_CATEGORY_ID,
    MSG_TYPE_CREATED_BY,
	MSG_TYPE_CREATION_DATE,
	MSG_TYPE_LAST_MODIFIED_BY,
	MSG_TYPE_LAST_MODIFIED_DATE,
	MSG_LOG_ID,
	BODID,
	MSG_STATUS_ID,
	REFERENCE_ID,
	MSG_LOG_CREATE_DATE,
	MSG_LOG_CREATED_BY,
	MSG_LOG_CREATION_DATE,
	MSG_TEXT_ID,
	MSG_CLOB FROM IDENTIFIER (:V_RESPONSE)
WHERE msg_status_id = 2;







CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_COMPAS_REPORTING)
(
messageType VARCHAR(16777216),
referenceID VARCHAR(16777216),
requestCreatedBy VARCHAR(16777216),
ReasonRemarks VARCHAR(16777216)
);


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_OUR_LOG)
(
messageType VARCHAR(16777216),
referenceID VARCHAR(16777216),
timeOfRequest VARCHAR(16777216),
timeOfResponse VARCHAR(16777216),
requestCreatedBy VARCHAR(16777216),
ReasonRemarks VARCHAR(16777216)
);


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_RESPONSE));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);




V_STEP := ''STEP2'';
   
V_STEP_NAME := ''Inserting all records having failed response for a successful request''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
SELECT 
SReq.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    SReq.MSG_DESC AS Req_MSG_DESC,
	SReq.INBOUND AS Req_INBOUND,
	SReq.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    SReq.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	SReq.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	SReq.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	SReq.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	SReq.MSG_LOG_ID AS Req_MSG_LOG_ID,
	SReq.BODID AS Req_BODID,
	SReq.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	SReq.REFERENCE_ID AS Req_REFERENCE_ID,
	SReq.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	SReq.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	SReq.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	SReq.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	SReq.MSG_CLOB AS Req_MSG_CLOB,
    FRes.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    FRes.MSG_DESC AS Res_MSG_DESC,
	FRes.INBOUND AS Res_INBOUND,
	FRes.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    FRes.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	FRes.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	FRes.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	FRes.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	FRes.MSG_LOG_ID AS Res_MSG_LOG_ID,
	FRes.BODID AS Res_BODID,
	FRes.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	FRes.REFERENCE_ID AS Res_REFERENCE_ID,
	FRes.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	FRes.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	FRes.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	FRes.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	FRes.MSG_CLOB  AS Res_MSG_CLOB
FROM IDENTIFIER (:V_SUCCESSFUL_REQUEST) SReq
RIGHT OUTER JOIN IDENTIFIER (:V_FAIL_RESPONSE) FRes
ON (Req_REFERENCE_ID = Res_REFERENCE_ID);


--INSERTING INTO COMPAS_REPORTING

INSERT INTO IDENTIFIER (:V_COMPAS_REPORTING)
SELECT Res_MSG_TYPE_ID, Res_REFERENCE_ID, Req_MSG_LOG_CREATED_BY, ''Failed Status 1'' FROM IDENTIFIER (:V_CTE)
WHERE Req_REFERENCE_ID IS NOT NULL;

--INSERTING INTO OUR_LOG

INSERT INTO IDENTIFIER (:V_OUR_LOG)
SELECT Res_MSG_TYPE_ID, Res_REFERENCE_ID, Req_MSG_LOG_CREATE_DATE, Res_MSG_LOG_CREATE_DATE, Req_MSG_LOG_CREATED_BY, ''Failed Response'' FROM IDENTIFIER (:V_CTE)
WHERE REQ_REFERENCE_ID IS NOT NULL;

        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_OUR_LOG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);





V_STEP := ''STEP3'';
   
V_STEP_NAME := ''Inserting all records having failed response for no successful request''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
--INSERTING INTO OUR_LOG
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
SELECT 
    SReq.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    SReq.MSG_DESC AS Req_MSG_DESC,
	SReq.INBOUND AS Req_INBOUND,
	SReq.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    SReq.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	SReq.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	SReq.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	SReq.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	SReq.MSG_LOG_ID AS Req_MSG_LOG_ID,
	SReq.BODID AS Req_BODID,
	SReq.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	SReq.REFERENCE_ID AS Req_REFERENCE_ID,
	SReq.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	SReq.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	SReq.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	SReq.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	SReq.MSG_CLOB AS Req_MSG_CLOB,
    FRes.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    FRes.MSG_DESC AS Res_MSG_DESC,
	FRes.INBOUND AS Res_INBOUND,
	FRes.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    FRes.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	FRes.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	FRes.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	FRes.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	FRes.MSG_LOG_ID AS Res_MSG_LOG_ID,
	FRes.BODID AS Res_BODID,
	FRes.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	FRes.REFERENCE_ID AS Res_REFERENCE_ID,
	FRes.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	FRes.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	FRes.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	FRes.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	FRes.MSG_CLOB  AS Res_MSG_CLOB
FROM IDENTIFIER (:V_SUCCESSFUL_REQUEST) SReq
RIGHT OUTER JOIN IDENTIFIER (:V_FAIL_RESPONSE) FRes
ON (Req_REFERENCE_ID = Res_REFERENCE_ID);

INSERT INTO IDENTIFIER (:V_OUR_LOG)
SELECT Res_MSG_TYPE_ID, Res_REFERENCE_ID, Req_MSG_LOG_CREATION_DATE, ''NA'', Req_MSG_LOG_CREATED_BY, ''Failed response for no successful request in data'' FROM IDENTIFIER (:V_CTE)
WHERE Req_REFERENCE_ID IS  NULL;



V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_OUR_LOG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);






V_STEP := ''STEP4'';
   
V_STEP_NAME := ''Inserting all successful req-res pairs under time limit''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
--INSERTING INTO OUR_LOG
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
    SELECT SReq.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    SReq.MSG_DESC AS Req_MSG_DESC,
	SReq.INBOUND AS Req_INBOUND,
	SReq.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    SReq.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	SReq.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	SReq.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	SReq.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	SReq.MSG_LOG_ID AS Req_MSG_LOG_ID,
	SReq.BODID AS Req_BODID,
	SReq.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	SReq.REFERENCE_ID AS Req_REFERENCE_ID,
	SReq.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	SReq.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	SReq.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	SReq.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	SReq.MSG_CLOB AS Req_MSG_CLOB,
    SRes.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    SRes.MSG_DESC AS Res_MSG_DESC,
	SRes.INBOUND AS Res_INBOUND,
	SRes.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    SRes.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	SRes.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	SRes.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	SRes.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	SRes.MSG_LOG_ID AS Res_MSG_LOG_ID,
	SRes.BODID AS Res_BODID,
	SRes.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	SRes.REFERENCE_ID AS Res_REFERENCE_ID,
	SRes.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	SRes.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	SRes.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	SRes.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	SRes.MSG_CLOB  AS Res_MSG_CLOB
    FROM IDENTIFIER (:V_SUCCESSFUL_REQUEST) SReq
    INNER JOIN IDENTIFIER (:V_SUCCESSFUL_RESPONSE) SRes
    ON (Req_REFERENCE_ID = Res_REFERENCE_ID);
  

INSERT INTO IDENTIFIER (:V_OUR_LOG)
SELECT Res_MSG_TYPE_ID, Res_REFERENCE_ID, Req_MSG_LOG_CREATION_DATE, Res_MSG_LOG_CREATION_DATE, Req_MSG_LOG_CREATED_BY, ''Response within Time limit'' FROM IDENTIFIER (:V_CTE)
WHERE DATEDIFF(minute, Req_MSG_LOG_CREATION_DATE, Res_MSG_LOG_CREATION_DATE) < :TIME_OUT_IN_MINUTES;

        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_OUR_LOG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
 
                                 

                                 
                                 
V_STEP := ''STEP5'';
   
V_STEP_NAME := ''Inserting all successful req-res pairs over time limit''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
--INSERTING INTO OUR_LOG
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
    SELECT SReq.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    SReq.MSG_DESC AS Req_MSG_DESC,
	SReq.INBOUND AS Req_INBOUND,
	SReq.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    SReq.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	SReq.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	SReq.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	SReq.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	SReq.MSG_LOG_ID AS Req_MSG_LOG_ID,
	SReq.BODID AS Req_BODID,
	SReq.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	SReq.REFERENCE_ID AS Req_REFERENCE_ID,
	SReq.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	SReq.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	SReq.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	SReq.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	SReq.MSG_CLOB AS Req_MSG_CLOB,
    SRes.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    SRes.MSG_DESC AS Res_MSG_DESC,
	SRes.INBOUND AS Res_INBOUND,
	SRes.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    SRes.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	SRes.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	SRes.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	SRes.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	SRes.MSG_LOG_ID AS Res_MSG_LOG_ID,
	SRes.BODID AS Res_BODID,
	SRes.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	SRes.REFERENCE_ID AS Res_REFERENCE_ID,
	SRes.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	SRes.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	SRes.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	SRes.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	SRes.MSG_CLOB  AS Res_MSG_CLOB
    FROM IDENTIFIER (:V_SUCCESSFUL_REQUEST) SReq
    INNER JOIN IDENTIFIER (:V_SUCCESSFUL_RESPONSE) SRes
    ON (Req_REFERENCE_ID = Res_REFERENCE_ID);

INSERT INTO IDENTIFIER (:V_OUR_LOG)
SELECT Res_msg_type_id, Res_reference_id, Req_msg_log_creation_date, Res_msg_log_creation_date, Req_msg_log_created_by, ''Late response. timelimit exceeded'' FROM IDENTIFIER (:V_CTE)
WHERE DATEDIFF(minute, Req_msg_log_creation_date, Res_msg_log_creation_date) > :TIME_OUT_IN_MINUTES;



--INSERTING INTO COMPAS_REPORTING

    
INSERT INTO IDENTIFIER (:V_COMPAS_REPORTING)
SELECT Res_MSG_TYPE_ID, Res_reference_id, Req_msg_log_created_by, ''Delayed'' FROM IDENTIFIER (:V_CTE)
WHERE DATEDIFF(minute, Req_msg_log_creation_date, Res_msg_log_creation_date) > :TIME_OUT_IN_MINUTES;

        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_OUR_LOG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);




V_STEP := ''STEP6'';
   
V_STEP_NAME := ''Inserting success responses and failed responses in exclusion list''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
--INSERTING INTO OUR_LOG
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
    SELECT FReq.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    FReq.MSG_DESC AS Req_MSG_DESC,
	FReq.INBOUND AS Req_INBOUND,
	FReq.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    FReq.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	FReq.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	FReq.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	FReq.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	FReq.MSG_LOG_ID AS Req_MSG_LOG_ID,
	FReq.BODID AS Req_BODID,
	FReq.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	FReq.REFERENCE_ID AS Req_REFERENCE_ID,
	FReq.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	FReq.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	FReq.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	FReq.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	FReq.MSG_CLOB AS Req_MSG_CLOB,
    SRes.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    SRes.MSG_DESC AS Res_MSG_DESC,
	SRes.INBOUND AS Res_INBOUND,
	SRes.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    SRes.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	SRes.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	SRes.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	SRes.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	SRes.MSG_LOG_ID AS Res_MSG_LOG_ID,
	SRes.BODID AS Res_BODID,
	SRes.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	SRes.REFERENCE_ID AS Res_REFERENCE_ID,
	SRes.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	SRes.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	SRes.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	SRes.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	SRes.MSG_CLOB  AS Res_MSG_CLOB
    FROM IDENTIFIER (:V_FAIL_REQUEST) FReq
    INNER JOIN IDENTIFIER (:V_SUCCESSFUL_RESPONSE) SRes
    ON (Req_reference_id = Res_reference_id);

INSERT INTO IDENTIFIER (:V_OUR_LOG)
SELECT Req_msg_type_id, Res_reference_id, ''NA'', Res_msg_log_creation_date, ''NA'', concat(''Failed status 1 but error acceptable as in ERROREXCLUSIONS_errorCode:'', (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''ReasonCode''):"$"), ''and errorReason:'', (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''Description''):"$")) FROM IDENTIFIER (:V_CTE)
WHERE (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''Description''):"$") IN (SELECT * FROM IDENTIFIER (:V_CMP_UPDATEPAYMENT_ERROREXCLUSION));

        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_OUR_LOG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);




V_STEP := ''STEP7'';
   
V_STEP_NAME := ''Inserting success responses and failed responses not in exclusion list''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
--INSERTING INTO OUR_LOG
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
    SELECT FReq.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    FReq.MSG_DESC AS Req_MSG_DESC,
	FReq.INBOUND AS Req_INBOUND,
	FReq.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    FReq.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	FReq.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	FReq.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	FReq.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	FReq.MSG_LOG_ID AS Req_MSG_LOG_ID,
	FReq.BODID AS Req_BODID,
	FReq.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	FReq.REFERENCE_ID AS Req_REFERENCE_ID,
	FReq.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	FReq.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	FReq.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	FReq.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	FReq.MSG_CLOB AS Req_MSG_CLOB,
    SRes.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    SRes.MSG_DESC AS Res_MSG_DESC,
	SRes.INBOUND AS Res_INBOUND,
	SRes.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    SRes.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	SRes.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	SRes.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	SRes.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	SRes.MSG_LOG_ID AS Res_MSG_LOG_ID,
	SRes.BODID AS Res_BODID,
	SRes.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	SRes.REFERENCE_ID AS Res_REFERENCE_ID,
	SRes.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	SRes.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	SRes.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	SRes.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	SRes.MSG_CLOB  AS Res_MSG_CLOB
    FROM IDENTIFIER (:V_FAIL_REQUEST) FReq
    INNER JOIN IDENTIFIER (:V_SUCCESSFUL_RESPONSE) SRes
    ON (Req_reference_id = Res_reference_id);

INSERT INTO IDENTIFIER (:V_OUR_LOG)
SELECT Req_msg_type_id, Res_reference_id, ''NA'', Res_msg_log_creation_date, Req_msg_log_created_by, concat(''Failed Status 1 with Response errorCode:'', (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''ReasonCode''):"$"), ''and errorReason:'', (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''Description''):"$")) FROM IDENTIFIER (:V_CTE)
WHERE (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''Description''):"$") NOT IN (SELECT * FROM IDENTIFIER (:V_CMP_UPDATEPAYMENT_ERROREXCLUSION));



--INSERTING INTO COMPAS_REPORTING

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
    SELECT FReq.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    FReq.MSG_DESC AS Req_MSG_DESC,
	FReq.INBOUND AS Req_INBOUND,
	FReq.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    FReq.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	FReq.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	FReq.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	FReq.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	FReq.MSG_LOG_ID AS Req_MSG_LOG_ID,
	FReq.BODID AS Req_BODID,
	FReq.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	FReq.REFERENCE_ID AS Req_REFERENCE_ID,
	FReq.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	FReq.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	FReq.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	FReq.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	FReq.MSG_CLOB AS Req_MSG_CLOB,
    SRes.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    SRes.MSG_DESC AS Res_MSG_DESC,
	SRes.INBOUND AS Res_INBOUND,
	SRes.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    SRes.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	SRes.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	SRes.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	SRes.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	SRes.MSG_LOG_ID AS Res_MSG_LOG_ID,
	SRes.BODID AS Res_BODID,
	SRes.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	SRes.REFERENCE_ID AS Res_REFERENCE_ID,
	SRes.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	SRes.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	SRes.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	SRes.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	SRes.MSG_CLOB  AS Res_MSG_CLOB
    FROM IDENTIFIER (:V_FAIL_REQUEST) FReq
    INNER JOIN IDENTIFIER (:V_SUCCESSFUL_RESPONSE) SRes
    ON (Req_reference_id = Res_reference_id);

INSERT INTO IDENTIFIER (:V_COMPAS_REPORTING)
SELECT Req_msg_type_id, Res_reference_id, Req_msg_log_created_by, concat(''Failed Status 1 with Response errorCode:'', (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''ReasonCode''):"$"), ''and errorReason:'', (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''Description''):"$")) FROM IDENTIFIER (:V_CTE)
WHERE (XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(XMLGET(Req_msg_clob, ''DataArea''),''BOD''),''NounOutcome''),''NounFailure''),''ErrorMessage''),''Description''):"$") NOT IN (SELECT * FROM IDENTIFIER (:V_CMP_UPDATEPAYMENT_ERROREXCLUSION));



        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_OUR_LOG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);





V_STEP := ''STEP8'';
   
V_STEP_NAME := ''Inserting success responses for which there is not request in the last 2 batches''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
--INSERTING INTO OUR_LOG
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
    SELECT Req.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    Req.MSG_DESC AS Req_MSG_DESC,
	Req.INBOUND AS Req_INBOUND,
	Req.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    Req.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	Req.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	Req.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	Req.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	Req.MSG_LOG_ID AS Req_MSG_LOG_ID,
	Req.BODID AS Req_BODID,
	Req.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	Req.REFERENCE_ID AS Req_REFERENCE_ID,
	Req.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	Req.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	Req.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	Req.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	Req.MSG_CLOB AS Req_MSG_CLOB,
    SRes.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    SRes.MSG_DESC AS Res_MSG_DESC,
	SRes.INBOUND AS Res_INBOUND,
	SRes.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    SRes.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	SRes.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	SRes.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	SRes.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	SRes.MSG_LOG_ID AS Res_MSG_LOG_ID,
	SRes.BODID AS Res_BODID,
	SRes.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	SRes.REFERENCE_ID AS Res_REFERENCE_ID,
	SRes.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	SRes.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	SRes.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	SRes.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	SRes.MSG_CLOB  AS Res_MSG_CLOB
    FROM IDENTIFIER (:V_REQUEST) Req
    RIGHT OUTER JOIN IDENTIFIER (:V_SUCCESSFUL_RESPONSE) SRes
    ON (Req_reference_id = Res_reference_id);

INSERT INTO IDENTIFIER (:V_OUR_LOG)
with log as(
SELECT  Res_MSG_TYPE_ID,
x.Res_reference_id, ''NA'', x.Res_msg_log_creation_date, ''NA'', ''No request for this reference ID within time limit spread across 2 batches of load'' FROM IDENTIFIER (:V_CTE) x
WHERE x.Req_reference_id IS NULL)
 
SELECT Request,log.Res_reference_id, ''NA'', log.Res_msg_log_creation_date, ''NA'', ''No request for this reference ID within time limit spread across 2 batches of load''  
FROM IDENTIFIER (:V_REQ_RES_PAIR) lookup INNER JOIN log 
ON lookup.response=log.Res_MSG_TYPE_ID;

        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_OUR_LOG));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);






V_STEP := ''STEP9'';
   
V_STEP_NAME := ''Inserting requests having no responses in the data to request_buffer''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
TRUNCATE IDENTIFIER (:V_REQUEST_BUFFER); --To remove the data if present before loading

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_CTE) AS
    SELECT SReq.MSG_TYPE_ID AS Req_MSG_TYPE_ID,
    SReq.MSG_DESC AS Req_MSG_DESC,
	SReq.INBOUND AS Req_INBOUND,
	SReq.MSG_CATEGORY_ID AS Req_MSG_CATEGORY_ID,
    SReq.MSG_TYPE_CREATED_BY AS Req_MSG_TYPE_CREATED_BY,
	SReq.MSG_TYPE_CREATION_DATE AS Req_MSG_TYPE_CREATION_DATE,
	SReq.MSG_TYPE_LAST_MODIFIED_BY AS Req_MSG_TYPE_LAST_MODIFIED_BY,
	SReq.MSG_TYPE_LAST_MODIFIED_DATE AS Req_MSG_TYPE_LAST_MODIFIED_DATE,
	SReq.MSG_LOG_ID AS Req_MSG_LOG_ID,
	SReq.BODID AS Req_BODID,
	SReq.MSG_STATUS_ID AS Req_MSG_STATUS_ID,
	SReq.REFERENCE_ID AS Req_REFERENCE_ID,
	SReq.MSG_LOG_CREATE_DATE AS Req_MSG_LOG_CREATE_DATE,
	SReq.MSG_LOG_CREATED_BY AS Req_MSG_LOG_CREATED_BY,
	SReq.MSG_LOG_CREATION_DATE AS Req_MSG_LOG_CREATION_DATE,
	SReq.MSG_TEXT_ID AS Req_MSG_TEXT_ID,
	SReq.MSG_CLOB AS Req_MSG_CLOB,
    Res.MSG_TYPE_ID AS Res_MSG_TYPE_ID,
    Res.MSG_DESC AS Res_MSG_DESC,
	Res.INBOUND AS Res_INBOUND,
	Res.MSG_CATEGORY_ID AS Res_MSG_CATEGORY_ID,
    Res.MSG_TYPE_CREATED_BY AS Res_MSG_TYPE_CREATED_BY,
	Res.MSG_TYPE_CREATION_DATE AS Res_MSG_TYPE_CREATION_DATE,
	Res.MSG_TYPE_LAST_MODIFIED_BY AS Res_MSG_TYPE_LAST_MODIFIED_BY,
	Res.MSG_TYPE_LAST_MODIFIED_DATE AS Res_MSG_TYPE_LAST_MODIFIED_DATE,
	Res.MSG_LOG_ID AS Res_MSG_LOG_ID,
	Res.BODID AS Res_BODID,
	Res.MSG_STATUS_ID AS Res_MSG_STATUS_ID,
	Res.REFERENCE_ID AS Res_REFERENCE_ID,
	Res.MSG_LOG_CREATE_DATE AS Res_MSG_LOG_CREATE_DATE,
	Res.MSG_LOG_CREATED_BY AS Res_MSG_LOG_CREATED_BY,
	Res.MSG_LOG_CREATION_DATE AS Res_MSG_LOG_CREATION_DATE,
	Res.MSG_TEXT_ID AS Res_MSG_TEXT_ID,
	Res.MSG_CLOB  AS Res_MSG_CLOB
    FROM IDENTIFIER (:V_SUCCESSFUL_REQUEST) SReq
    LEFT OUTER JOIN IDENTIFIER (:V_RESPONSE) res
    ON (Req_reference_id = Res_reference_id);

INSERT INTO IDENTIFIER (:V_COMPAS_REPORTING)
SELECT Req_MSG_TYPE_ID, Req_REFERENCE_ID, Req_MSG_LOG_CREATED_BY, ''Missing/Delayed'' FROM IDENTIFIER (:V_CTE)
WHERE 
Res_REFERENCE_ID IS NULL
AND DATEDIFF(minute, Req_msg_log_creation_date, (SELECT MAX(MSG_LOG_CREATION_DATE) FROM IDENTIFIER (:V_SUCCESSFUL_REQUEST)) ) > :TIME_OUT_IN_MINUTES;



INSERT INTO IDENTIFIER (:V_OUR_LOG)
SELECT Req_MSG_TYPE_ID, Req_REFERENCE_ID, Req_msg_log_creation_date, ''N/A'', Req_msg_log_created_by, ''No response within time limit spread across 2 batches of load/1 batch if reaches timelimit'' FROM IDENTIFIER (:V_CTE)
WHERE 
Res_REFERENCE_ID IS NULL
AND DATEDIFF(minute, Req_msg_log_creation_date, (SELECT MAX(MSG_LOG_CREATION_DATE) FROM IDENTIFIER (:V_SUCCESSFUL_REQUEST))) > :TIME_OUT_IN_MINUTES; 



INSERT INTO IDENTIFIER (:V_REQUEST_BUFFER)
SELECT Req_MSG_TYPE_ID,
    Req_MSG_DESC,
	Req_INBOUND,
	Req_MSG_CATEGORY_ID,
    Req_MSG_TYPE_CREATED_BY,
	Req_MSG_TYPE_CREATION_DATE,
	Req_MSG_TYPE_LAST_MODIFIED_BY,
	Req_MSG_TYPE_LAST_MODIFIED_DATE,
	Req_MSG_LOG_ID,
	Req_BODID,
	Req_MSG_STATUS_ID,
	Req_REFERENCE_ID,
	Req_MSG_LOG_CREATE_DATE,
	Req_MSG_LOG_CREATED_BY,
	Req_MSG_LOG_CREATION_DATE,
	Req_MSG_TEXT_ID,
	Req_MSG_CLOB FROM IDENTIFIER (:V_CTE)
WHERE Res_REFERENCE_ID IS NULL
AND DATEDIFF(minute, Req_msg_log_creation_date, (SELECT MAX(MSG_LOG_CREATION_DATE) FROM IDENTIFIER (:V_SUCCESSFUL_REQUEST))) < :TIME_OUT_IN_MINUTES;

        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_REQUEST_BUFFER));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);





V_STEP := ''STEP10'';

 
V_STEP_NAME := ''Generate Compas_Reporting file''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/cmp_msg_plan/UpdatePaymentRequestResponse/compasReporting/''||''requestResponseReport''||DATE(CURRENT_TIMESTAMP())|| ''_''||HOUR(CURRENT_TIMESTAMP())||'':''||MINUTE(CURRENT_TIMESTAMP())||''.csv''||'' FROM (
             SELECT *
               FROM COMPAS_REPORTING
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               compression = None             
               )
               
               
               
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

execute immediate ''USE SCHEMA ''||:TGT_SC;                          
execute immediate :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);




V_STEP := ''STEP11'';

 
V_STEP_NAME := ''Generate Out_Log file''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/cmp_msg_plan/UpdatePaymentRequestResponse/UpdatePaymentLogs/''||''log''||DATE(CURRENT_TIMESTAMP())|| ''_''||HOUR(CURRENT_TIMESTAMP())||'':''||MINUTE(CURRENT_TIMESTAMP())||''.txt''||'' FROM (
             SELECT *
               FROM OUR_LOG
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               compression = None             
               )
               
               
               
HEADER = True
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