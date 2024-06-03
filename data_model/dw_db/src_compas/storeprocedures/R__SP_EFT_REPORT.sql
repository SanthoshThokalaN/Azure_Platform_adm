USE SCHEMA SRC_COMPAS;
CREATE OR REPLACE PROCEDURE SP_EFT_REPORT("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "LZ_COMPAS_SC" VARCHAR(16777216), "LZ_ISDW_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_REPORT_TIME    VARCHAR    := TO_VARCHAR(CURRENT_TIMESTAMP(), ''yyyymmddHH24MISS'');

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''SP_EFT_REPORT'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''SP_EFT_REPORT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;
  

V_Eft_Isdw_Import                   VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.Eft_Isdw_Import'';
V_Temp_Report_From_Log_Ind          VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.Temp_Report_From_Log_Ind'';
V_Temp_Report_From_Log_Mem          VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.Temp_Report_From_Log_Mem''; 

V_Isdw_Temp                         VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.Isdw_Temp'';
V_Final_Report_v1                   VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.Final_Report_v1'';
V_Final_Report_v2                   VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.Final_Report_v2'';
V_Final_Report                      VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.Final_Report'';

V_finalReportMSCombo                VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.finalReportMSCombo'';
V_finalReportHIP                    VARCHAR     :=      :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.finalReportHIP'';



BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';


V_STEP := ''STEP1'';
   
V_STEP_NAME := ''Lpadding membership number in ISDW import and storing temporarily''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());





CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_Isdw_Temp) AS
SELECT 
HOUSEHOLD_ID,
IFF((length(Membership_Number) = 9), Membership_Number, LPAD(Membership_Number, 9,''0'')) AS MembershipNumber,
individualCountForHousehold,
countEmailForHousehold,
Individual_ID,
Email_addr AS EmailAddress,
plan_type_id
FROM IDENTIFIER (:V_Eft_Isdw_Import);

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_Isdw_Temp));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);









V_STEP := ''STEP2'';
   
V_STEP_NAME := ''Appending joined ISDW imported data with Ind and Mem temp reports from EFT_Extract''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_Final_Report_v1) AS
SELECT DISTINCT
r_ind.msg_status_id,
r_ind.individual_id,
r_isdw.membershipnumber,
r_ind.frequency,
r_ind.paymentamount,
r_ind.paymenttype,
r_ind.msg_log_creation_date,
r_isdw.household_id,
r_isdw.individualcountforhousehold,
r_isdw.countemailforhousehold,
r_isdw.emailaddress,
r_isdw.plan_type_id
FROM
IDENTIFIER(:V_Temp_Report_From_Log_Ind) r_ind JOIN IDENTIFIER (:V_Isdw_Temp) r_isdw
ON r_ind.individual_id = r_isdw.individual_id
UNION
SELECT DISTINCT
r_mem.msg_status_id,
r_isdw.individual_id,
r_mem.membershipnumber,
r_mem.frequency,
r_mem.paymentamount,
r_mem.paymenttype,
r_mem.msg_log_creation_date,
r_isdw.household_id,
r_isdw.individualcountforhousehold,
r_isdw.countemailforhousehold,
r_isdw.emailaddress,
r_isdw.plan_type_id
FROM
IDENTIFIER(:V_Temp_Report_From_Log_Mem) r_mem JOIN IDENTIFIER (:V_Isdw_Temp) r_isdw
ON r_mem.MembershipNumber = r_isdw.MembershipNumber;

        

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_Final_Report_v1));

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);







V_STEP := ''STEP3'';
   
V_STEP_NAME := ''Populating Email IDs for household with more than 1 individuals and 1 email id along with ungrouped population of plan_type''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());




CREATE OR REPLACE Temporary TABLE IDENTIFIER (:V_Final_Report_v2) AS
SELECT 
t1.membershipnumber AS MembershipNumber,
t1.plan_type_id AS plan_type_id,
t1.individualcountforhousehold as individualcountforhousehold,
t1.countemailforhousehold as countemailforhousehold,
COALESCE(t1.EMAILADDRESS, t2.EMAILADDRESS) AS EMAILADDRESS,
IFF(t1.plan_type_id IN (''1'', ''5'', ''6'', ''7'', ''8'', ''10'', ''14'', ''16'', ''17'', ''9'', ''15'', ''3''), ''Med Supp/Med Supp and HIP Combo'', IFF(t1.plan_type_id IN (''2'', ''4'', ''11'', ''13''), ''HIP'', (''Med Supp/Med Supp and HIP Combo''))) AS PlanType,
t1.paymenttype as PaymentType,
t1.paymentamount as PaymentAmount,
t1.frequency as frequency,
t1.msg_log_creation_date as msg_log_creation_date
FROM
    IDENTIFIER (:V_Final_Report_v1) t1
LEFT JOIN
    IDENTIFIER (:V_Final_Report_v1) t2 ON t1.Household_id = t2.Household_id AND t2.EMAILADDRESS IS NOT NULL;

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_Final_Report_v2));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);







V_STEP := ''STEP4'';
   
V_STEP_NAME := ''Populating plan type on MemberNumber grouping and creating the final report''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_Final_Report) AS
SELECT  
IFF((length(MembershipNumber) >= 9), MembershipNumber, LPAD(MembershipNumber, 9,''0'')) AS MembershipNumber,
IFF(CONTAINS((SELECT LISTAGG(PLANTYPE) FROM IDENTIFIER (:V_Final_Report_v2) f WHERE f.MEMBERSHIPNUMBER = t.MEMBERSHIPNUMBER), ''Med Supp/Med Supp and HIP Combo''), ''Med Supp/Med Supp and HIP Combo'', ''HIP'') AS PLANTYPE,
EmailAddress,
PaymentType,
PaymentAmount,
frequency,
msg_log_creation_date
FROM IDENTIFIER (:V_Final_Report_v2) t;

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_Final_Report));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);







V_STEP := ''STEP5'';
   
V_STEP_NAME := ''Creating MSCombo Table for reporting''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_finalReportMSCombo) AS
SELECT DISTINCT
MembershipNumber,
PlanType,
EmailAddress,
PaymentType,
PaymentAmount
FROM IDENTIFIER(:V_Final_Report)
WHERE PlanType = ''Med Supp/Med Supp and HIP Combo'';

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_finalReportMSCombo));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);









V_STEP := ''STEP6'';
   
V_STEP_NAME := ''Creating HIP Table for reporting''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_finalReportHIP) AS
SELECT DISTINCT
MembershipNumber,
PlanType,
EmailAddress,
PaymentType,
PaymentAmount
FROM IDENTIFIER(:V_Final_Report)
WHERE PlanType = ''HIP'';

 


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_finalReportHIP));
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);




                                 







V_STEP := ''STEP5'';

 
V_STEP_NAME := ''Generate final Report for MSCombo PlanType''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/cmp_msg_plan/1x_EFT_CC_Payment/EFT_Extract_Outbound_Reports/''||''Email_1xCCEFTPayment_MSCOMBO_''||:V_REPORT_TIME||''.csv''||'' FROM (
             SELECT 
             MembershipNumber AS "MembershipNumber",
             PlanType AS "PlanType",
             EmailAddress AS "EmailAddress",
             PaymentType AS "PaymentType",
             PaymentAmount AS "PaymentAmount"
               FROM finalReportMSCombo
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               compression = None
               null_if = (''''(null)'''', ''''\\N'''')
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




V_STEP := ''STEP6'';

 
V_STEP_NAME := ''Generate final Report for PlanType = HIP''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/cmp_msg_plan/1x_EFT_CC_Payment/EFT_Extract_Outbound_Reports/''||''Email_1xCCEFTPayment_HIP_''||:V_REPORT_TIME||''.csv''||'' FROM (
            SELECT
             MembershipNumber AS "MembershipNumber",
             PlanType AS "PlanType",
             EmailAddress AS "EmailAddress",
             PaymentType AS "PaymentType",
             PaymentAmount AS "PaymentAmount"
               FROM finalReportHIP
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               compression = None 
               null_if = (''''(null)'''', ''''\\N'''')
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
