USE SCHEMA SRC_FOX;

CREATE OR REPLACE PROCEDURE SP_REPORT_WITH_DUP_CLAIMS("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "LZ_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_DUPLICATE_CLAIMS_REPORT_DETAIL      	VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_FOX'') || ''.duplicate_claims_report_detail'';

V_PROCESS_NAME   VARCHAR DEFAULT ''DUPLICATE CLAIMS'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''REPORT_WITH_DUP_CLAIMS'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY1      VARCHAR;

V_STAGE_QUERY2      VARCHAR;
V_REPORT_DT         VARCHAR;

V_MOD VARCHAR := ''''''50'''',''''76'''',''''77'''',''''91'''',''''CO'''',''''CQ'''',''''GO'''',''''GP'''',''''E1'''',''''E2'''',''''E3'''',''''E4'''',''''FA'''',''''F1'''',''''F2'''',''''F3'''',''''F4'''',''''F5'''',''''F6'''',''''F7'''',''''F8'''',''''F9'''',''''LC'''',''''LD'''',''''LM'''',''''LT'''',''''RT'''',''''RC'''',''''RI'''',''''TA'''',''''T1'''',''''T2'''',''''T3'''',''''T4'''',''''T5'''',''''T6'''',''''T7'''',''''T8'''',''''19'''',''''T9'''',''''XE'''',''''XP'''',''''XS'''',''''XU'''',''''JW'''',''''AA'''',''''AD'''',''''QK'''',''''QX'''',''''QY'''',''''QZ'''',''''80'''',''''81'''',''''82'''',''''AS'''',''''DD'''',''''DE'''',''''DG'''',''''DH'''',''''DI'''',''''DN'''',''''DP'''',''''DR'''',''''DS'''',''''DX'''',''''ED'''',''''EE'''',''''EG'''',''''EH'''',''''EI'''',''''EJ'''',''''EN'''',''''EP'''',''''ER'''',''''ES'''',''''EX'''',''''GD'''',''''GE'''',''''GG'''',''''GH'''',''''GI'''',''''GJ'''',''''GN'''',''''GP'''',''''GR'''',''''GS'''',''''GX'''',''''HD'''',''''HE'''',''''HG'''',''''HH'''',''''HI'''',''''HN'''',''''HP'''',''''HR'''',''''HS'''',''''HX'''',''''ID'''',''''IE'''',''''IG'''',''''IH'''',''''IN'''',''''IP'''',''''IR'''',''''IS'''',''''IX'''',''''JD'''',''''JE'''',''''JG'''',''''JH'''',''''JI'''',''''JJ'''',''''JN'''',''''JP'''',''''JR'''',''''JS'''',''''JX'''',''''ND'''',''''NE'''',''''NG'''',''''NH'''',''''NI'''',''''NN'''',''''NP'''',''''NR'''',''''NS'''',''''NX'''',''''PD'''',''''PE'''',''''PG'''',''''PH'''',''''PI'''',''''PN'''',''''PP'''',''''PR'''',''''PS'''',''''PX'''',''''RD'''',''''RE'''',''''RG'''',''''RH'''',''''RI'''',''''RN'''',''''RP'''',''''RR'''',''''RS'''',''''RX'''',''''SD'''',''''SE'''',''''SG'''',''''SH'''',''''SI'''',''''SN'''',''''SP'''',''''SR'''',''''SS'''',''''SX'''',''''XD'''',''''XG'''',''''XH'''',''''XI'''',''''XR'''',''''XX'''''';



V_DUPLICATE_CLAIMS_REPORT_DATA_AFTER_MATCH       VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_FOX_SC,''LZ_FOX'') || ''.DUPLICATE_CLAIMS_REPORT_DATA_AFTER_MATCH'';
V_DUPLICATE_CLAIMS_REPORT_TEMP                   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_FOX_SC,''LZ_FOX'') || ''.DUPLICATE_CLAIMS_REPORT_TEMP'';
V_TMP_REPORT_WITH_DUP_CLAIMS                     VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_FOX'') || ''.TMP_REPORT_WITH_DUP_CLAIMS'';
    



BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';
V_REPORT_DT := (SELECT TO_VARCHAR(CURRENT_TIMESTAMP(2), ''YYYYMMDDHHMISS'')) ;
V_STEP := ''STEP1'';
   
 
V_STEP_NAME := ''Load REPORT_WITH_DUP_CLAIMS''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_REPORT_WITH_DUP_CLAIMS) AS 
SELECT
    distinct tb1.clm_num AS "Duplicate Claim Number",
    tb1.dt_cmpltd AS "Date Processed",
    LPAD(tb1.mem_id, 11, ''0'') AS "Membership Number",
    ''DupClaimsScreen7'' as "Report Name",
    tb1.OPER_ID as "Ions",
    tb1.srv_from_dt AS "Date of Service From",
    tb1.srv_to_dt AS "Date of Service To",
    tb1.cpt_cd AS "CPT",
    tb1.tin AS "TIN",
    tb1.bill_prv_npi AS "NPI",
    tb1.ben_amt AS "Benefit",
    tb2.clm_num as "Original Claim Number",
    tb1.partb_ded_amt as "Deductible",
    CASE WHEN tb1.pat_para_num = ''54609'' THEN ''Y'' ELSE ''N'' END AS "Message 54609"
FROM
    IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_TEMP) tb1
    JOIN IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_DATA_AFTER_MATCH) tb2
    ON ( 1= 1
    
AND   tb1.ben_amt= tb2.ben_amt AND tb1.pay_adj_amt= tb2.pay_adj_amt
   AND tb1.pl_of_srvc= tb2.pl_of_srvc
    AND tb1.srv_from_dt= tb2.srv_from_dt
   AND tb1.srv_to_dt= tb2.srv_to_dt
    AND tb1.cpt_cd= tb2.cpt_cd
   AND tb1.chrg_amt= tb2.chrg_amt
    AND tb1.partb_ded_amt= tb2.partb_ded_amt
   AND tb1.coins_amt= tb2.coins_amt
    AND tb1.mem_id = tb2.mem_id
    AND (tb1.medcr_clm_ctl_nbr=tb2.medcr_clm_ctl_nbr or tb1.clh_trk_id= tb2.clh_trk_id)
    AND ( tb1.rend_prv_npi= tb2.rend_prv_npi or tb1.bill_prv_npi= tb2.bill_prv_npi)
    AND (CASE WHEN tb1.proc_mod1 IN (:V_MOD) then tb1.proc_mod1 ELSE 0 END) = (CASE WHEN tb2.proc_mod1 IN (:V_MOD) then tb2.proc_mod1 ELSE 0 END)
    AND (CASE WHEN tb1.proc_mod2 IN (:V_MOD) then tb1.proc_mod2 ELSE 0 END)= (CASE WHEN tb2.proc_mod2 IN (:V_MOD) then tb2.proc_mod2 ELSE 0 END)
    AND (CASE WHEN tb1.proc_mod3 IN (:V_MOD) then tb1.proc_mod3 ELSE 0 END)= (CASE WHEN tb2.proc_mod3 IN (:V_MOD) then tb2.proc_mod3 ELSE 0 END) 
    )
    
    
    ;



    

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_REPORT_WITH_DUP_CLAIMS)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

                                 
                                 
V_STEP := ''STEP2'';

 
V_STEP_NAME := ''Generate Report for Duplicate Claims''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY1 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/duplicate_claims/''||''RPA_PI_DupClaimsScreen7_''||:V_REPORT_DT ||''.csv''||'' FROM (
             SELECT *
               FROM TMP_REPORT_WITH_DUP_CLAIMS
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

V_STAGE_QUERY2 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/duplicate_claims/''||''RPA_PI_DupClaimsScreen7_''||:V_REPORT_DT ||''.csv''||'' FROM (SELECT ''''Duplicate Claim Number'''',
''''Date Processed'''',
''''Membership Number'''' ,
''''Report Name'''',
''''Ions'''' ,
''''Date of Service From'''' ,
''''Date of Service To'''',
''''CPT'''' ,
''''TIN'''',
''''NPI'''' ,
''''Benefit'''',
''''Original Claim Number'''' ,
''''Deductible'''' ,
''''Message 54609'''')
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
               compression = None
                            
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
TRUNCATE TABLE IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_DETAIL) ;
 
-- Inserting Report Name and Rows Count into The Table.
INSERT INTO IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_DETAIL) VALUES (''RPA_PI_DupClaimsScreen7_''||:V_REPORT_DT||''.csv'', :V_ROWS_LOADED) ;


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
