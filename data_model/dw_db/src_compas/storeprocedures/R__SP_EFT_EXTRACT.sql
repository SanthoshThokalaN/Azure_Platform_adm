USE SCHEMA SRC_COMPAS;
CREATE OR REPLACE PROCEDURE SP_EFT_EXTRACT("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "LZ_COMPAS_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''SP_EFT_EXTRACT'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''SP_EFT_EXTRACT'';

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
V_REQ_RES_PAIR                      VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.REQ_RES_PAIR''; 

V_XML_CLEANED_STG_STRING            VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.XML_CLEANED_STG_STRING'';
V_DATA_FILTERED_READ                VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.DATA_FILTERED_READ'';

V_Individual_IDs_To_Extract         VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.Individual_IDs_To_Extract'';
V_Member_Numbers_To_Extract         VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.Member_Numbers_To_Extract'';
V_Temp_Report_From_Log_Ind          VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.Temp_Report_From_Log_Ind'';
V_Temp_Report_From_Log_Mem          VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.Temp_Report_From_Log_Mem'';




BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';


V_STEP := ''STEP1'';
   
V_STEP_NAME := ''Creating a string casted msg_clob''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());





CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_XML_CLEANED_STG_STRING) AS
SELECT msg_type_id, msg_desc, inbound, msg_category_id,
                            msg_type_created_by, msg_type_creation_date, msg_type_last_modified_by,
                            msg_type_last_modified_date,
                            msg_log_id, bodid, msg_status_id, reference_id,
                            msg_log_create_date, msg_log_created_by, msg_log_creation_date, msg_text_id,
                            TO_VARCHAR(msg_clob) as msg_clob FROM IDENTIFIER (:V_XML_CLEANED_STG);

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_XML_CLEANED_STG_STRING));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);









V_STEP := ''STEP2'';
   
V_STEP_NAME := ''Creating data_filtered_read table from the msg_clob xml elements''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   



CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_DATA_FILTERED_READ) AS
SELECT msg_type_id, msg_desc, inbound, msg_category_id,
                            msg_type_created_by, msg_type_creation_date, msg_type_last_modified_by,
                            msg_type_last_modified_date,
                            msg_log_id, bodid, msg_status_id, reference_id,
                            msg_log_create_date, msg_log_created_by, msg_log_creation_date, msg_text_id, msg_clob, 

CASE

WHEN
CONTAINS(msg_clob, ''<IndividualId>'') = TRUE
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<IndividualId>'', 1) + length(''<IndividualId>'')), (REGEXP_INSTR(msg_clob, ''</IndividualId>'', 1)) - (REGEXP_INSTR(msg_clob, ''<IndividualId>'', 1) + length(''<IndividualId>''))) != ''''
THEN
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<IndividualId>'', 1) + length(''<IndividualId>'')), (REGEXP_INSTR(msg_clob, ''</IndividualId>'', 1)) - (REGEXP_INSTR(msg_clob, ''<IndividualId>'', 1) + length(''<IndividualId>'')))

WHEN
CONTAINS(msg_clob, ''<IndividualId>'') = TRUE
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<IndividualId>'', 1) + length(''<IndividualId>'')), (REGEXP_INSTR(msg_clob, ''</IndividualId>'', 1)) - (REGEXP_INSTR(msg_clob, ''<IndividualId>'', 1) + length(''<IndividualId>''))) = ''''
THEN
NULL

WHEN
CONTAINS(msg_clob, ''<IndividualId>'') = FALSE
THEN
NULL
END AS IndividualId,

CASE
WHEN
CONTAINS(msg_clob, ''<MemberNumber>'') = TRUE
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<MemberNumber>'', 1) + length(''<MemberNumber>'')), (REGEXP_INSTR(msg_clob, ''</MemberNumber>'', 1)) - (REGEXP_INSTR(msg_clob, ''<MemberNumber>'', 1) + length(''<MemberNumber>''))) != ''''
THEN
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<MemberNumber>'', 1) + length(''<MemberNumber>'')), (REGEXP_INSTR(msg_clob, ''</MemberNumber>'', 1)) - (REGEXP_INSTR(msg_clob, ''<MemberNumber>'', 1) + length(''<MemberNumber>'')))

WHEN
CONTAINS(msg_clob, ''<MemberNumber>'') = TRUE
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<MemberNumber>'', 1) + length(''<MemberNumber>'')), (REGEXP_INSTR(msg_clob, ''</MemberNumber>'', 1)) - (REGEXP_INSTR(msg_clob, ''<MemberNumber>'', 1) + length(''<MemberNumber>''))) = ''''
THEN
NULL

WHEN
CONTAINS(msg_clob, ''<MemberNumber>'') = FALSE
THEN
NULL
END AS MemberNumber,


CASE
WHEN
CONTAINS(msg_clob, ''<Frequency>'') = TRUE
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>'')), (REGEXP_INSTR(msg_clob, ''</Frequency>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>''))) != ''''
THEN
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>'')), (REGEXP_INSTR(msg_clob, ''</Frequency>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>'')))

WHEN
CONTAINS(msg_clob, ''<Frequency>'') = TRUE
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>'')), (REGEXP_INSTR(msg_clob, ''</Frequency>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>''))) = ''''
THEN
NULL

WHEN
CONTAINS(msg_clob, ''<Frequency>'') = FALSE
THEN
NULL
END AS Frequency,



CASE
WHEN
CONTAINS(msg_clob, ''<Amount>'') = TRUE
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Amount>'', 1) + length(''<Amount>'')), (REGEXP_INSTR(msg_clob, ''</Amount>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Amount>'', 1) + length(''<Amount>''))) != ''''
THEN
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Amount>'', 1) + length(''<Amount>'')), (REGEXP_INSTR(msg_clob, ''</Amount>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Amount>'', 1) + length(''<Amount>'')))

WHEN
CONTAINS(msg_clob, ''<Amount>'') = TRUE
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Amount>'', 1) + length(''<Amount>'')), (REGEXP_INSTR(msg_clob, ''</Amount>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Amount>'', 1) + length(''<Amount>''))) = ''''
THEN
NULL

WHEN
CONTAINS(msg_clob, ''<Amount>'') = FALSE
THEN
NULL
END AS Amount,


CASE
WHEN MSG_TYPE_ID = ''501022''
AND
CONTAINS (msg_clob, ''<EftAction>'') = TRUE
AND ((REGEXP_INSTR(msg_clob, ''<EftAction>'', 1)) > (REGEXP_INSTR(msg_clob, ''<UpdateType'', 1)))
AND ((REGEXP_INSTR(msg_clob, ''</EftAction>'', 1)) < (REGEXP_INSTR(msg_clob, ''</UpdateType>'', 1)))
AND ((REGEXP_INSTR(msg_clob, ''<Frequency>'', 1)) > (REGEXP_INSTR(msg_clob, ''<EftAction>'', 1)))
AND ((REGEXP_INSTR(msg_clob, ''<Frequency>'', 1)) < (REGEXP_INSTR(msg_clob, ''</EftAction>'', 1)))
AND SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>'')), (REGEXP_INSTR(msg_clob, ''</Frequency>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>''))) = ''2''
THEN
''Electronic Funds Transfer (EFT)''

 
WHEN MSG_TYPE_ID = ''501022''
AND
CONTAINS (msg_clob, ''<CreditCardAction>'') = TRUE
AND ((REGEXP_INSTR(msg_clob, ''<CreditCardAction>'', 1)) > (REGEXP_INSTR(msg_clob, ''<UpdateType'', 1)))
AND ((REGEXP_INSTR(msg_clob, ''</CreditCardAction>'', 1)) < (REGEXP_INSTR(msg_clob, ''</UpdateType>'', 1)))
AND CONTAINS(msg_clob, ''<Frequency>'') = TRUE
AND ((REGEXP_INSTR(msg_clob, ''<Frequency>'', 1)) > (REGEXP_INSTR(msg_clob, ''<CreditCardAction>'', 1)))
AND ((REGEXP_INSTR(msg_clob, ''<Frequency>'', 1)) < (REGEXP_INSTR(msg_clob, ''</CreditCardAction>'', 1)))
AND SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>'')), (REGEXP_INSTR(msg_clob, ''</Frequency>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>''))) = ''''
THEN
''Credit Card''


WHEN MSG_TYPE_ID = ''101022''
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Task>'', 1) + length(''<Task>'')), (REGEXP_INSTR(msg_clob, ''</Task>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Task>'', 1) + length(''<Task>''))) = ''Order''
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>'')), (REGEXP_INSTR(msg_clob, ''</Frequency>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Frequency>'', 1) + length(''<Frequency>''))) = ''2''
THEN
''Electronic Funds Transfer (EFT)''


WHEN MSG_TYPE_ID = ''101022''
AND
SUBSTR(msg_clob, (REGEXP_INSTR(msg_clob, ''<Task>'', 1) + length(''<Task>'')), (REGEXP_INSTR(msg_clob, ''</Task>'', 1)) - (REGEXP_INSTR(msg_clob, ''<Task>'', 1) + length(''<Task>''))) = ''Credit Card Payment''
THEN
''Credit Card''
END AS PaymentType,

msg_status_id AS Status
FROM IDENTIFIER (:V_XML_CLEANED_STG_STRING)
WHERE msg_type_id IN (SELECT Request FROM IDENTIFIER (:V_REQ_RES_PAIR));


        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_DATA_FILTERED_READ));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);







V_STEP := ''STEP3'';
   
V_STEP_NAME := ''Creating a list of individual IDs to extract''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


TRUNCATE TABLE IDENTIFIER (:V_Individual_IDs_To_Extract);
INSERT INTO IDENTIFIER (:V_Individual_IDs_To_Extract)
SELECT DISTINCT concat(''('',''\\''magic\\'','', IndividualId, '')'') AS IndividualIds FROM IDENTIFIER (:V_DATA_FILTERED_READ)
WHERE
((Frequency != ''1'') OR (Frequency IS NULL))
AND
Amount IS NOT NULL
AND
((Status != 1) OR (Status IS NULL))
AND
IndividualId IS NOT NULL;


 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_Individual_IDs_To_Extract));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);














V_STEP := ''STEP4'';
   
V_STEP_NAME := ''Creating a list of member numbers to extract''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());




TRUNCATE TABLE IDENTIFIER (:V_Member_Numbers_To_Extract);

INSERT INTO IDENTIFIER (:V_Member_Numbers_To_Extract)
SELECT DISTINCT concat(''('',''\\''magic\\'','', MemberNumber, '')'') AS MemberNos FROM IDENTIFIER (:V_DATA_FILTERED_READ)
WHERE
((Frequency != ''1'') OR (Frequency IS NULL))
AND
Amount IS NOT NULL
AND
((Status != 1) OR (Status IS NULL))
AND
MemberNumber IS NOT NULL;

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_Member_Numbers_To_Extract));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);







V_STEP := ''STEP5'';
   
V_STEP_NAME := ''Creating temporary reports from COMPAS Log extract based on Individual_ID''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());




TRUNCATE TABLE IDENTIFIER (:V_Temp_Report_From_Log_Ind);
INSERT INTO IDENTIFIER (:V_Temp_Report_From_Log_Ind)
--CREATE OR REPLACE TRANSIENT TABLE IDENTIFIER (:V_Temp_Report_From_Log_Ind) AS
SELECT
msg_status_id,
IndividualId AS individual_id,
MemberNumber AS MembershipNumber,
Frequency,
Amount AS PaymentAmount,
PaymentType,
msg_log_creation_date FROM IDENTIFIER (:V_DATA_FILTERED_READ)
WHERE
((Frequency != ''1'') OR (Frequency IS NULL))
AND
Amount IS NOT NULL
AND
PaymentType IS NOT NULL
AND
((Status != 1) OR (Status IS NULL))
AND
IndividualId IS NOT NULL;

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_Temp_Report_From_Log_Ind));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);









V_STEP := ''STEP6'';
   
V_STEP_NAME := ''Creating temporary reports from COMPAS Log extract based on Member_Number''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());




TRUNCATE TABLE IDENTIFIER (:V_Temp_Report_From_Log_Mem);
INSERT INTO IDENTIFIER (:V_Temp_Report_From_Log_Mem)
--CREATE OR REPLACE TRANSIENT TABLE IDENTIFIER (:V_Temp_Report_From_Log_Mem) AS
SELECT
msg_status_id,
IndividualId AS individual_id,
MemberNumber AS MembershipNumber,
Frequency,
Amount AS PaymentAmount,
PaymentType,
msg_log_creation_date FROM IDENTIFIER (:V_DATA_FILTERED_READ)
WHERE
((Frequency != ''1'') OR (Frequency IS NULL))
AND
Amount IS NOT NULL
AND
PaymentType IS NOT NULL
AND
((Status != 1) OR (Status IS NULL))
AND
MemberNumber IS NOT NULL;

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_Temp_Report_From_Log_Mem));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);









V_STEP := ''STEP7'';

 
V_STEP_NAME := ''Generate Individual temp report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/cmp_msg_plan/1x_EFT_CC_Payment/EFT_Extract_Temp_Reports/''||''temp_report_from_log_ind''||''.csv''||'' FROM (
             SELECT *
               FROM Temp_Report_From_Log_Ind
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''~''''
               compression = None
               NULL_IF = ()
               )
               
               
               
HEADER = False
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

execute immediate ''USE SCHEMA ''||:LZ_COMPAS_SC;                          
execute immediate :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);




V_STEP := ''STEP8'';

 
V_STEP_NAME := ''Generate Member_Number temp report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/cmp_msg_plan/1x_EFT_CC_Payment/EFT_Extract_Temp_Reports/''||''temp_report_from_log_mem''||''.csv''||'' FROM (
             SELECT *
               FROM Temp_Report_From_Log_Mem
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''~''''
               compression = None 
               NULL_IF = ()
               )
               
               
               
HEADER = False
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

execute immediate ''USE SCHEMA ''||:LZ_COMPAS_SC;                          
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