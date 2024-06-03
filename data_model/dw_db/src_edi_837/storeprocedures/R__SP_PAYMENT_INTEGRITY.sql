USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE "SP_PAYMENT_INTEGRITY"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''PAYMENT_INTEGRITY'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT ''PAYMENT_INTEGRITY'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;
V_STAGE_QUERY_HEADER     VARCHAR;
V_START_DATE_TRANSACTSET VARCHAR;
V_END_DATE_TRANSACTSET VARCHAR;

V_TMP_PLAN_N_HDR_ANALYSIS                VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PLAN_N_HDR_ANALYSIS'';
V_TMP_PLAN_N_HDR_DELIVERY       VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PLAN_N_HDR_DELIVERY'';
V_TMP_WALGREEN_HDR_ANALYSIS                VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_WALGREEN_HDR_ANALYSIS'';
V_TMP_WALGREEN_HDR_DELIVERY       VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_WALGREEN_HDR_DELIVERY'';

 
    
V_INST_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLAIM_PART'';
V_FOX_POSTADJ_IMPORT         VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.FOX_POSTADJ_IMPORT'';

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_START_DATE_TRANSACTSET := (select TO_VARCHAR(DATEADD(DAY,-21,:V_CURRENT_DATE)::DATE,''YYYYMMDD''));

V_END_DATE_TRANSACTSET := (select TO_VARCHAR(DATEADD(DAY,3,:V_CURRENT_DATE)::DATE,''YYYYMMDD''));

V_STEP := ''STEP1'';
   
 
V_STEP_NAME := ''Load TMP_PLAN_N_HDR_ANALYSIS''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   



CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_PLAN_N_HDR_ANALYSIS) AS 

WITH domain_filter AS
(
  SELECT CLM_NUM,
  BIL_LN_NUM,
  dt_cmpltd,
  PLN_CD,
  compas_pln_cd,
  chrg_amt,
  MCARE_PD_AMT,
  ben_amt,
  MCARE_APRVD_AMT,
  AARP_COPAY_AMT,
  OOP_AMT,
  oper_id,
  srv_from_dt,
  srv_to_dt,
  acct_nbr,
  tos_cd,
  CLM_NOTE_DAT,
  plsrv_cd,
  CLH_TRK_ID,
  MEDCR_CLM_CTL_NBR,
  ecSource,
  clmSource,
  tin,
  NAT_PROV_ID,
  PAT_PARA_NUM,
  doc_ctl_nbr
  FROM identifier(:v_fox_postadj_import)
  WHERE pln_cd IN (''N'',''NW'',''NW1'',''NW*'',''NS'',''NS1'',''NS*'',''N01'',''N*1'')
  AND aarp_copay_amt = 0
  AND (
  (
  (CLM_NOTE_DAT NOT LIKE ''%COVID%''
        AND CLM_NOTE_DAT NOT LIKE ''%Covid%''
        AND CLM_NOTE_DAT NOT LIKE ''%covid%'')
  OR CLM_NOTE_DAT IS NULL
  )
  
        AND PAT_PARA_NUM <> ''92900'')
  AND ben_amt > 0
  AND dt_cmpltd BETWEEN :V_CURRENT_DATE AND :V_CURRENT_DATE
  AND clmsource like ''I%''
),
history_filter AS
(
SELECT DISTINCT
CLM_NUM,
dt_cmpltd,
srv_from_dt,
tos_cd,
PLSRV_CD,
acct_nbr
FROM identifier(:v_fox_postadj_import)
WHERE (tos_cd = ''H2'' OR plsrv_cd = ''21'')
  AND dt_cmpltd >= add_months(:V_CURRENT_DATE,-8) -- still to parameterize the month
  AND ACCT_NBR IN (SELECT ACCT_NBR FROM domain_filter)
),
sameService AS
(
SELECT DISTINCT df.ACCT_NBR AS ACCT_NBR
        FROM domain_filter df
        JOIN history_filter hf
                ON df.acct_nbr = hf.acct_nbr
WHERE df.srv_from_dt = hf.srv_from_dt
      OR df.srv_from_dt = dateadd(DAY,-1,hf.srv_from_dt)),
final_domain_filter AS
(SELECT df.*
FROM domain_filter df
WHERE acct_nbr NOT IN (SELECT ACCT_NBR FROM sameService))
SELECT DISTINCT
clm.app_sender_code,
fox.CLM_NUM,
fox.BIL_LN_NUM,
fox.dt_cmpltd,
fox.PLN_CD,
fox.compas_pln_cd,
fox.chrg_amt,
fox.MCARE_PD_AMT,
fox.ben_amt,
fox.MCARE_APRVD_AMT,
fox.aarp_copay_amt,
fox.oop_amt,
fox.oper_id,
fox.srv_from_dt,
fox.srv_to_dt,
fox.acct_nbr,
--mem.subscriber_id,
fox.tos_cd,
fox.CLM_NOTE_DAT,
fox.PLSRV_CD,
fox.clmSource,
fox.tin,
fox.NAT_PROV_ID,
clm.total_claim_charge_amt,
clm.healthcareservice_location,
clm.health_care_code_info,
clm.sl_seq_num,
clm.product_service_id_qlfr,
clm.line_item_charge_amt,
clm.transactset_create_date,
''CH'' AS EDP_SOURCE,
MIN(sl_seq_num) over (partition by clm_num) MAX_SL_SEQ_NUM,
PAT_PARA_NUM
FROM final_domain_filter fox
  JOIN identifier(:v_inst_claim_part) clm
    ON fox.CLH_TRK_ID = clm.network_trace_number
    --AND clm.sl_seq_num = fox.BIL_LN_NUM 
WHERE clm.transactset_create_date BETWEEN TO_VARCHAR(DATEADD(DAY,-21,:V_CURRENT_DATE)::DATE,''YYYYMMDD'') AND TO_VARCHAR(DATEADD(DAY,3,:V_CURRENT_DATE)::DATE,''YYYYMMDD'')
    AND split(product_service_id_qlfr,'':'')[1] in (''99281'', ''99282'', ''99283'', ''99284'',''99285'')
    AND patient_status_code NOT IN (''09'',''02'',''20'',''66'')

UNION

SELECT DISTINCT
clm.app_sender_code,
fox.CLM_NUM,
fox.BIL_LN_NUM,
fox.dt_cmpltd,
fox.PLN_CD,
fox.compas_pln_cd,
fox.chrg_amt,
fox.MCARE_PD_AMT,
fox.ben_amt,
fox.MCARE_APRVD_AMT,
fox.aarp_copay_amt,
fox.oop_amt,
fox.oper_id,
fox.srv_from_dt,
fox.srv_to_dt,
fox.acct_nbr,
fox.tos_cd,
fox.CLM_NOTE_DAT,
fox.PLSRV_CD,
fox.clmSource,
fox.tin,
fox.NAT_PROV_ID,
clm.total_claim_charge_amt,
clm.healthcareservice_location,
clm.health_care_code_info,
clm.sl_seq_num,
clm.product_service_id_qlfr,
clm.line_item_charge_amt,
clm.transactset_create_date,
''EXELA'' AS EDP_SOURCE,
MIN(sl_seq_num) over (partition by clm_num) MAX_SL_SEQ_NUM,
PAT_PARA_NUM
FROM identifier(:v_inst_claim_part) clm
JOIN final_domain_filter fox
    ON lpad(substr(fox.clm_num,1,11),11,''0'') =  trim(substr(clm.clm_billing_note_text,2,12))
WHERE clm.transactset_create_date BETWEEN :V_START_DATE_TRANSACTSET AND :V_END_DATE_TRANSACTSET
    AND clm.app_sender_code = ''EXELA''
    AND split(product_service_id_qlfr,'':'')[1] in (''99281'', ''99282'', ''99283'', ''99284'',''99285'')
    AND patient_status_code NOT IN (''09'',''02'',''20'',''66'')

UNION

SELECT DISTINCT
clm.app_sender_code,
fox.CLM_NUM,
fox.BIL_LN_NUM,
fox.dt_cmpltd,
fox.PLN_CD,
fox.compas_pln_cd,
fox.chrg_amt,
fox.MCARE_PD_AMT,
fox.ben_amt,
fox.MCARE_APRVD_AMT,
fox.aarp_copay_amt,
fox.oop_amt,
fox.oper_id,
fox.srv_from_dt,
fox.srv_to_dt,
fox.acct_nbr,
fox.tos_cd,
fox.CLM_NOTE_DAT,
fox.PLSRV_CD,
fox.clmSource,
fox.tin,
fox.NAT_PROV_ID,
clm.total_claim_charge_amt,
clm.healthcareservice_location,
clm.health_care_code_info,
clm.sl_seq_num,
clm.product_service_id_qlfr,
clm.line_item_charge_amt,
clm.transactset_create_date,
''EXELA'' AS EDP_SOURCE,
MIN(sl_seq_num) over (partition by clm_num) AS MAX_SL_SEQ_NUM,
PAT_PARA_NUM
FROM identifier(:v_inst_claim_part) clm
JOIN final_domain_filter fox
    ON fox.doc_ctl_nbr =  trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12))
WHERE clm.transactset_create_date BETWEEN :V_START_DATE_TRANSACTSET AND :V_END_DATE_TRANSACTSET
    AND clm.app_sender_code = ''EXELA''
    AND split(product_service_id_qlfr,'':'')[1] in (''99281'', ''99282'', ''99283'', ''99284'',''99285'')
    AND patient_status_code NOT IN (''09'',''02'',''20'',''66'');



  

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PLAN_N_HDR_ANALYSIS)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
  
  
  
  V_STEP := ''STEP2''; 

 
V_STEP_NAME := ''Generate File for PlanN Header Analsysis Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/payment_integrity/planN/''||''RPA_PI_PlanNCopay_''||(select TO_VARCHAR(:V_CURRENT_DATE,''YYYYMMDD''))||''_analysis''||'' FROM (
             			 SELECT *
               FROM  TMP_PLAN_N_HDR_ANALYSIS
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
			   empty_field_as_null=false
               NULL_IF = (''''NULL'''')
               compression = None
              )
               
               
               
HEADER = TRUE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

execute immediate ''USE SCHEMA ''||:TGT_SC;                          
execute immediate :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);   


 V_STEP := ''STEP3'';
   
   V_STEP_NAME := ''Load TMP_PLAN_N_HDR_DELIVERY''; 
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
   
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_PLAN_N_HDR_DELIVERY) AS  
  


SELECT DISTINCT LPAD(CLM_NUM,12,0) AS "Claim Number",
dt_cmpltd as "Date Processed",
LPAD(acct_nbr,11,0) AS "Membership Number",
''Plan N'' as "Report Name",
oper_id as "Ions",
srv_from_dt as "Date of Service From",
srv_to_dt as "Date of Service To",
split(product_service_id_qlfr,'':'')[1]::VARCHAR AS "CPT",
tin as "TIN",
NAT_PROV_ID as "NPI",
to_varchar(ben_amt) as "Benefit"
FROM IDENTIFIER(:V_TMP_PLAN_N_HDR_ANALYSIS)
WHERE MAX_SL_SEQ_NUM = sl_seq_num --to make sure single bill line is represented at final report to make sure only one line per claim
ORDER BY
 "Membership Number",
 "Date Processed",
 "Date of Service From"
  
    ;
  

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PLAN_N_HDR_DELIVERY)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
  
  
  
 V_STEP := ''STEP4'';
 
V_STEP_NAME := ''Generate File for PlanN Header Delivery Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());    
  
ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;   

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/payment_integrity/planN/''||''RPA_PI_PlanNCopay_''||(select TO_VARCHAR(:V_CURRENT_DATE,''YYYYMMDD''))||''.csv''||'' FROM (

  
			SELECT *
               FROM  TMP_PLAN_N_HDR_DELIVERY
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
			   empty_field_as_null=false
               NULL_IF = (''''NULL'''')
               compression = None
              )
               
               
           
               
               
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;




V_STAGE_QUERY_HEADER := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/payment_integrity/planN/''||''RPA_PI_PlanNCopay_''||(select TO_VARCHAR(:V_CURRENT_DATE,''YYYYMMDD''))||''.csv''||'' FROM (

  
SELECT 
''''Claim Number'''',
''''Date Processed'''',
''''Membership Number'''',
''''Report Name'''',
''''Ions'''',
''''Date of Service From'''',
''''Date of Service To'''',
''''CPT'''',
''''TIN'''',
''''NPI'''',
''''Benefit''''


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


IF (V_ROWS_LOADED > 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY;  
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

ELSEIF (V_ROWS_LOADED = 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY_HEADER;  
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

END IF;    
   
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);  
  
  
  
   V_STEP := ''STEP5'';
   
   V_STEP_NAME := ''Load TMP_WALGREEN_HDR_ANALYSIS''; 
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
  
  

   
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_WALGREEN_HDR_ANALYSIS) AS  
  
 WITH domain_filter AS
(
  SELECT 
  clm_num,
  clmsource,
  ecsource,
  ClaimSuspenseReason,
  dt_cmpltd,
  srv_from_dt,
  srv_to_dt,
  chrg_amt,
  MCARE_PD_AMT,
  MCARE_APRVD_AMT,
  PARTB_DED_AMT,
  ben_amt,
  cpt_cd,
  tin,
  NAT_PROV_ID,
  oper_id,
  medcr_clm_ctl_nbr,
  acct_nbr
  FROM identifier(:v_fox_postadj_import)
  WHERE dt_cmpltd >= add_months(:V_CURRENT_DATE,-8) -- still to parameterize the month
  AND tin IN (112731721,
          260456013,
          261924026,
          361920426,
          361924002,
          361924025,
          361924026,
          361924039,
          362127039,
          362312887,
          363796738,
          510099047,
          632312887,
          660232687,
          900904914,
          770515894)
  AND ben_amt <> 0
  AND BIL_LN_NUM=1),
claim_cohort AS
(SELECT
clm_num as orig_clm_num, clmsource as orig_clmsource, ecsource as orig_ecsource, ClaimSuspenseReason as orig_ClaimSuspenseReason, dt_cmpltd as orig_dt_cmpltd, srv_from_dt as orig_srv_from_dt, srv_to_dt as orig_srv_to_dt, chrg_amt as orig_chrg_amt, MCARE_PD_AMT as orig_MCARE_PD_AMT, MCARE_APRVD_AMT as orig_MCARE_APRVD_AMT, PARTB_DED_AMT as orig_PARTB_DED_AMT,  ben_amt as orig_ben_amt, cpt_cd as orig_cpt_cd, tin as orig_tin, NAT_PROV_ID as orig_NAT_PROV_ID, oper_id as orig_oper_id, medcr_clm_ctl_nbr as orig_medcr_clm_ctl_nbr,acct_nbr as orig_acct_nbr
FROM
    domain_filter
WHERE
    dt_cmpltd BETWEEN :V_CURRENT_DATE AND :V_CURRENT_DATE
    --AND clmsource like ''__CMS/CH''
)
  

 SELECT cc.*,hc.*
 FROM claim_cohort cc
 JOIN domain_filter hc
      ON cc.orig_acct_nbr = hc.acct_nbr
      AND trim(cc.orig_medcr_clm_ctl_nbr) = trim(hc.MEDCR_CLM_CTL_NBR)
      AND cc.orig_clm_num <> hc.clm_num
      AND cc.orig_dt_cmpltd > hc.dt_cmpltd
WHERE
    hc.dt_cmpltd <= :V_CURRENT_DATE
    AND trim(hc.MEDCR_CLM_CTL_NBR) IS NOT NULL
    AND trim(cc.orig_MEDCR_CLM_CTL_NBR) IS NOT NULL
   -- AND hc.MEDCR_CLM_CTL_NBR NOT LIKE ''null''
   AND hc.MEDCR_CLM_CTL_NBR IS NOT NULL
  
    AND hc.clmsource <> cc.orig_clmsource
    AND cc.orig_clmsource NOT LIKE ''%-CMS/CH'' AND cc.orig_clmsource NOT LIKE ''%-CLH'' AND cc.orig_clmsource NOT LIKE ''%-MNL'' and cc.orig_clmsource NOT LIKE ''%-CMS''
ORDER BY
  cc.orig_medcr_clm_ctl_nbr,
  cc.orig_ACCT_NBR,
  cc.orig_dt_cmpltd,
  cc.orig_srv_from_dt;

  

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_WALGREEN_HDR_ANALYSIS)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
  
 
 
 
 V_STEP := ''STEP6'';
 
V_STEP_NAME := ''Generate File for Walgreen Header Analysis Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/payment_integrity/walgreendups/''||''RPA_PI_WalgreensDups_''||(select TO_VARCHAR(:V_CURRENT_DATE,''YYYYMMDD''))||''_analysis''||'' FROM (
             SELECT *
               FROM  TMP_WALGREEN_HDR_ANALYSIS
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
			   empty_field_as_null=false
               NULL_IF = (''''NULL'''')
               compression = None
              )
               
               
               
HEADER = TRUE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;

execute immediate ''USE SCHEMA ''||:TGT_SC;                          
execute immediate :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);                                   
                                 
 
  
     V_STEP := ''STEP7'';
   
   V_STEP_NAME := ''Load TMP_WALGREEN_HDR_DELIVERY''; 
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
   
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_WALGREEN_HDR_DELIVERY) AS  

  
  SELECT DISTINCT LPAD(CLM_NUM,12,0) AS "Duplicate Claim Number",
dt_cmpltd as "Date Processed",
LPAD(orig_acct_nbr,11,0) AS "Membership Number",
''Walgreens Dups'' as "Report Name",
oper_id as "Ions",
srv_from_dt as "Date of Service From",
srv_to_dt as "Date of Service To",
cpt_cd as "CPT",
tin as "TIN",
NAT_PROV_ID as "NPI",
to_varchar(ben_amt) as "Benefit",
LPAD(orig_CLM_NUM,12,0) AS "Original Claim Number"
FROM identifier(:V_TMP_WALGREEN_HDR_ANALYSIS)
ORDER BY
 "Membership Number",
 "Date Processed",
 "Date of Service From"
 ;
  

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_WALGREEN_HDR_DELIVERY)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
                                 

                                 
V_STEP := ''STEP8'';
 
V_STEP_NAME := ''Generate File for Walgreen Header Delivery Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/payment_integrity/walgreendups/''||''RPA_PI_WalgreensDups_''||(select TO_VARCHAR(:V_CURRENT_DATE,''YYYYMMDD''))||''.csv''||'' FROM (

			 SELECT *
               FROM  TMP_WALGREEN_HDR_DELIVERY
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
			   empty_field_as_null=false
               NULL_IF = (''''NULL'''')
               compression = None
              )
               
               
               
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''

;


V_STAGE_QUERY_HEADER := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/payment_integrity/walgreendups/''||''RPA_PI_WalgreensDups_''||(select TO_VARCHAR(:V_CURRENT_DATE,''YYYYMMDD''))||''.csv''||'' FROM (

SELECT 
''''Duplicate Claim Number'''',
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
''''Original Claim Number''''

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


IF (V_ROWS_LOADED > 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY;  
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

ELSEIF (V_ROWS_LOADED = 0) THEN 

execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY_HEADER;  
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

END IF;   
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);                                   
                                 


EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
