USE Schema SRC_EDI_837 ;

CREATE OR REPLACE PROCEDURE SP_DUPLICATE_CLAIMS_2_FINAL_REPORT_GEN("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "LZ_FOX_SC" VARCHAR(16777216), "LZ_ISDW_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME             VARCHAR        default ''DUPLICATE_CLAIMS_II'';
V_SUB_PROCESS_NAME         VARCHAR        default ''DUPLICATE_CLAIMS2_FINAL_REPORT_GEN'';
V_STEP                     VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER; 
V_ROWS_LOADED              INTEGER; 
V_MESSAGE                  VARCHAR;
V_LAST_QUERY_ID            VARCHAR; 
V_STAGE_QUERY1             VARCHAR;
V_STAGE_QUERY2             VARCHAR;
V_REPORT_DT                VARCHAR;

V_DUPLICATE_CLAIMS_2_REPORT_TEMP			VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_FOX_SC,''LZ_FOX'') || ''.duplicate_claims_2_report_temp'';
V_DUPLICATE_CLAIMS_2_REPORT_ISDW_TEMP		VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.duplicate_claims_2_report_isdw_temp'';
V_DUP_CLAIMS2_REPORT_FINAL_TBL      	    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.dup_claims2_report_final_tbl'';
V_DUPLICATE_CLAIMS_2_REPORT_DETAIL      	VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.duplicate_claims_2_report_detail'';
    

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
 
V_STEP_NAME := ''Load DUP_CLAIMS2_REPORT_FINAL_TBL''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_REPORT_DT := (SELECT TO_VARCHAR(CURRENT_TIMESTAMP(2), ''YYYYMMDDHHMISS'')) ;

CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_DUP_CLAIMS2_REPORT_FINAL_TBL)  AS

SELECT
DISTINCT 
FOX_TMP_TBL.CLM_NUM AS "Duplicate Claim Number",
FOX_TMP_TBL.DT_CMPLTD AS "Date Processed",
LPAD(FOX_TMP_TBL.MEM_ID, 11, ''0'') AS "Membership Number",
''DupClaimsScreen1'' AS "Report Name",
FOX_TMP_TBL.OPER_ID AS "Ions",
FOX_TMP_TBL.SRV_FROM_DT AS "Date of Service From",
FOX_TMP_TBL.SRV_TO_DT AS "Date of Service To",
FOX_TMP_TBL.CPT_CD AS "CPT",
FOX_TMP_TBL.TIN AS "TIN",
FOX_TMP_TBL.BILL_PRV_NPI AS "NPI",
FOX_TMP_TBL.BEN_AMT AS "Benefit",
ISDW_TMP_TBL.CLM_NUM AS "Original Claim Number",
'''' AS "Deductible",
'''' AS "MESSAGE 54609"
FROM IDENTIFIER(:V_DUPLICATE_CLAIMS_2_REPORT_TEMP) FOX_TMP_TBL
JOIN 
IDENTIFIER(:V_DUPLICATE_CLAIMS_2_REPORT_ISDW_TEMP) ISDW_TMP_TBL
        ON (
            ((FOX_TMP_TBL.pl_of_srvc = ISDW_TMP_TBL.pl_of_srvc) or (FOX_TMP_TBL.pl_of_srvc is NULL AND ISDW_TMP_TBL.pl_of_srvc is NULL))
        AND (((FOX_TMP_TBL.srv_from_dt = ISDW_TMP_TBL.srv_from_dt) or (FOX_TMP_TBL.srv_from_dt is NULL and ISDW_TMP_TBL.srv_from_dt is NULL))
                or ((FOX_TMP_TBL.srv_to_dt = ISDW_TMP_TBL.srv_to_dt) or (FOX_TMP_TBL.srv_to_dt is NULL and ISDW_TMP_TBL.srv_to_dt is NULL)))
        AND (((FOX_TMP_TBL.TOT_BEN = ISDW_TMP_TBL.TOT_BEN) or (FOX_TMP_TBL.TOT_BEN is NULL and ISDW_TMP_TBL.TOT_BEN is NULL))
                or ((FOX_TMP_TBL.bill_prv_npi = ISDW_TMP_TBL.bill_prv_npi) or (FOX_TMP_TBL.bill_prv_npi is NULL AND ISDW_TMP_TBL.bill_prv_npi is NULL)))
        AND FOX_TMP_TBL.mem_id = ISDW_TMP_TBL.mem_id )   
;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_DUP_CLAIMS2_REPORT_FINAL_TBL)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
V_STEP := ''STEP2'';

 
V_STEP_NAME := ''Generate Report for DUPLICATE CLAIMS Scenario II Extract''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY1 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/duplicate_scenario_2/''||''RPA_PI_DupClaimsScreen1_''||:V_REPORT_DT||''.csv''||'' FROM (
             SELECT *
               FROM SRC_EDI_837.DUP_CLAIMS2_REPORT_FINAL_TBL
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               compression = None
                empty_field_as_null=false
                null_if = (''''null'''', ''''\\N'''')
               )
               
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;                                 

V_STAGE_QUERY2 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/duplicate_scenario_2/''||''RPA_PI_DupClaimsScreen1_''||:V_REPORT_DT||''.csv''||'' FROM 
(
SELECT ''''Duplicate Claim Number'''',
''''Date Processed'''',
''''Membership Number'''',
''''Report Name'''',
''''Ions'''',
''''Date of Service From'''',
''''Date of Service To'''',
''''CPT'''',
''''TIN'''',
''''NPI'''',
''''Benefit'''',
''''Original Claim Number'''',
''''Deductible'''',
''''Message 54609''''
)
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               compression = None
               empty_field_as_null=false
               null_if = (''''null'''', ''''\\N'''')
              )

HEADER = False
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;

IF (V_ROWS_LOADED > 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY1;  
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

ELSEIF (V_ROWS_LOADED = 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY2;  
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

END IF;   

-- TRUNCATING Table.
TRUNCATE TABLE IDENTIFIER(:V_DUPLICATE_CLAIMS_2_REPORT_DETAIL) ;

-- Inserting Report Name and Rows Count into The Table.
INSERT INTO IDENTIFIER(:V_DUPLICATE_CLAIMS_2_REPORT_DETAIL) VALUES (''RPA_PI_DupClaimsScreen1_''||:V_REPORT_DT||''.csv'', :V_ROWS_LOADED) ;

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);                                      

EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
