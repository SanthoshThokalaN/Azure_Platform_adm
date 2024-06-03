use schema SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_LEGACY_COMPAS_BILLING_TABLES("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), 
                                                            "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), 
                                                            "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_COMPAS_BILLING_TABLES'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_QUERY1 VARCHAR;
V_QUERY2 VARCHAR;
V_QUERY3 VARCHAR;
V_QUERY4 VARCHAR;
V_QUERY5 VARCHAR;
V_QUERY6 VARCHAR;
V_QUERY7 VARCHAR;
V_QUERY8 VARCHAR;
V_QUERY9 VARCHAR;
V_QUERY10 VARCHAR;
V_QUERY11 VARCHAR;
V_QUERY12 VARCHAR;
V_QUERY13 VARCHAR;
V_QUERY14 VARCHAR;
V_QUERY15 VARCHAR;
V_QUERY16 VARCHAR;

V_TEMP_TBL_billing_dallas  VARCHAR := :TGT_SC||''.''||''TMP_TBL_billing_dallas'';
V_SRC_TBL_billing_dallas   VARCHAR := :TGT_SC||''.''||''billing_dallas'';
V_TEMP_TBL_billing_eftauth  VARCHAR := :TGT_SC||''.''||''TMP_TBL_billing_eftauth'';
V_SRC_TBL_billing_eftauth   VARCHAR := :TGT_SC||''.''||''billing_eftauth'';
V_TEMP_TBL_billing_jpmceftd VARCHAR := :TGT_SC||''.''||''TMP_TBL_billing_jpmceftd'';
V_SRC_TBL_billing_jpmceftd  VARCHAR := :TGT_SC||''.''||''billing_jpmceftd'';
V_TEMP_TBL_billing_jpmceftm VARCHAR := :TGT_SC||''.''||''TMP_TBL_billing_jpmceftm'';
V_SRC_TBL_billing_jpmceftm  VARCHAR := :TGT_SC||''.''||''billing_jpmceftm'';
V_TEMP_TBL_billing_noc VARCHAR := :TGT_SC||''.''||''TMP_TBL_billing_noc'';
V_SRC_TBL_billing_noc  VARCHAR := :TGT_SC||''.''||''billing_noc'';
V_TEMP_TBL_billing_pitts VARCHAR := :TGT_SC||''.''||''TMP_TBL_billing_pitts'';
V_SRC_TBL_billing_pitts  VARCHAR := :TGT_SC||''.''||''billing_pitts'';
V_TEMP_TBL_billing_prot VARCHAR := :TGT_SC||''.''||''TMP_TBL_billing_prot'';
V_SRC_TBL_billing_prot  VARCHAR := :TGT_SC||''.''||''billing_prot'';
V_TEMP_TBL_billing_thdparty VARCHAR := :TGT_SC||''.''||''TMP_TBL_billing_thdparty'';
V_SRC_TBL_billing_thdparty  VARCHAR := :TGT_SC||''.''||''billing_thdparty'';

 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';

V_STEP_NAME := ''billing_dallas''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL_billing_dallas||'' AS 
(SELECT batch_type , 
  process_julian_date , 
  batch_number , 
  sequence_number , 
  transaction_code , 
  membership_number , 
  association_code , 
  coupon_date , 
  type_indicator , 
  check_number , 
  bank_account_routing_number , 
  employer_premium , 
  bank_account_number , 
  premium_amount , 
  membership_dues , 
  andrus_donation , 
  total_dollar_amount , 
  process_date
FROM ''||:V_SRC_TBL_billing_dallas||'' WHERE 1 = 2); ''; 

V_QUERY2 := ''  
COPY INTO ''||:V_TEMP_TBL_billing_dallas||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
,$9 
,$10 
,$11 
,$12
,$13 
,$14
,$15
,$16
,$17
,$18
from ''||:STAGE||''/billing_dallas)
FILE_FORMAT = ''''UTIL.FF_PIPE_CSV'''';''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 


INSERT INTO IDENTIFIER(:V_SRC_TBL_billing_dallas)
(
  batch_type , 
  process_julian_date , 
  batch_number , 
  sequence_number , 
  transaction_code , 
  membership_number , 
  association_code , 
  coupon_date , 
  type_indicator , 
  check_number , 
  bank_account_routing_number , 
  employer_premium , 
  bank_account_number , 
  premium_amount , 
  membership_dues , 
  andrus_donation , 
  total_dollar_amount , 
  process_date,
  ISDC_CREATED_DT,	
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  batch_type , 
  process_julian_date , 
  batch_number , 
  sequence_number , 
  transaction_code , 
  membership_number , 
  association_code , 
  coupon_date , 
  type_indicator , 
  check_number , 
  bank_account_routing_number , 
  employer_premium , 
  bank_account_number , 
  premium_amount , 
  membership_dues , 
  andrus_donation , 
  total_dollar_amount , 
  process_date,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL_billing_dallas)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, null, NULL, NULL);
                                 
V_STEP := ''STEP2'';

V_STEP_NAME := ''billing_eftauth''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

V_QUERY3 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL_billing_eftauth||'' AS 
(SELECT   process_date , 
  batch_number , 
  item_sequence_number , 
  bank_account_number , 
  bank_routing_number , 
  aarp_account_number , 
  aarp_association_code , 
  bank_account_type
FROM ''||:V_SRC_TBL_billing_eftauth||'' WHERE 1 = 2); ''; 

V_QUERY4 := ''  
COPY INTO ''||:V_TEMP_TBL_billing_eftauth||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
from  ''||:STAGE||''/billing_eftauth)
FILE_FORMAT = ''''UTIL.FF_PIPE_CSV'''';''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY3;  
execute immediate :V_QUERY4; 


INSERT INTO IDENTIFIER(:V_SRC_TBL_billing_eftauth)
(
    process_date , 
  batch_number , 
  item_sequence_number , 
  bank_account_number , 
  bank_routing_number , 
  aarp_account_number , 
  aarp_association_code , 
  bank_account_type,
  ISDC_CREATED_DT,	
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
    process_date , 
  batch_number , 
  item_sequence_number , 
  bank_account_number , 
  bank_routing_number , 
  aarp_account_number , 
  aarp_association_code , 
  bank_account_type,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL_billing_eftauth)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, null, NULL, NULL);
                                 
V_STEP := ''STEP3'';

V_STEP_NAME := ''billing_jpmceftd''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

V_QUERY5 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL_billing_jpmceftd||'' AS 
(SELECT     record_type , 
  transaction_code , 
  bank_routing_number , 
  dfi_account_number , 
  amount_billed , 
  aarp_account_number , 
  aarp_association_code , 
  individual_name_last_name , 
  individual_name_first_initial , 
  payment_type_code , 
  discretionary_data , 
  addenda_record_ind , 
  trace_number , 
  sequence_number , 
  process_date
FROM ''||:V_SRC_TBL_billing_jpmceftd||'' WHERE 1 = 2); ''; 

V_QUERY6 := ''  
COPY INTO ''||:V_TEMP_TBL_billing_jpmceftd||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
,$9 
,$10 
,$11 
,$12
,$13 
,$14
,$15
from  ''||:STAGE||''/billing_jpmceftd)
FILE_FORMAT = ''''UTIL.FF_PIPE_CSV'''';''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY5;  
execute immediate :V_QUERY6; 


INSERT INTO IDENTIFIER(:V_SRC_TBL_billing_jpmceftd)
(
   record_type , 
  transaction_code , 
  bank_routing_number , 
  dfi_account_number , 
  amount_billed , 
  aarp_account_number , 
  aarp_association_code,  
  individual_name_last_name , 
  individual_name_first_initial , 
  payment_type_code , 
  discretionary_data , 
  addenda_record_ind , 
  trace_number , 
  sequence_number , 
  process_date,
  ISDC_CREATED_DT,	
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  record_type , 
  transaction_code , 
  bank_routing_number , 
  dfi_account_number , 
  amount_billed , 
  aarp_account_number , 
  aarp_association_code , 
  individual_name_last_name , 
  individual_name_first_initial , 
  payment_type_code , 
  discretionary_data , 
  addenda_record_ind , 
  trace_number , 
  sequence_number , 
  process_date,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL_billing_jpmceftd)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, null, NULL, NULL);                                 


V_STEP := ''STEP4'';

V_STEP_NAME := ''billing_jpmceftm''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

V_QUERY7 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL_billing_jpmceftm||'' AS 
(SELECT     record_type , 
  transaction_code , 
  bank_routing_number , 
  dfi_account_number , 
  amount_billed , 
  aarp_account_number , 
  aarp_association_code , 
  individual_name_last_name , 
  individual_name_first_initial , 
  payment_type_code , 
  discretionary_data , 
  addenda_record_ind , 
  trace_number , 
  sequence_number , 
  process_date 
FROM ''||:V_SRC_TBL_billing_jpmceftm||'' WHERE 1 = 2); ''; 

V_QUERY8 := ''  
COPY INTO ''||:V_TEMP_TBL_billing_jpmceftm||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
,$9 
,$10 
,$11 
,$12
,$13 
,$14
,$15
from  ''||:STAGE||''/billing_jpmceftm)
FILE_FORMAT = ''''UTIL.FF_PIPE_CSV'''';''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY7;  
execute immediate :V_QUERY8; 


INSERT INTO IDENTIFIER(:V_SRC_TBL_billing_jpmceftm)
(
  record_type , 
  transaction_code , 
  bank_routing_number , 
  dfi_account_number , 
  amount_billed , 
  aarp_account_number , 
  aarp_association_code , 
  individual_name_last_name , 
  individual_name_first_initial , 
  payment_type_code , 
  discretionary_data , 
  addenda_record_ind , 
  trace_number , 
  sequence_number , 
  process_date ,
  ISDC_CREATED_DT,	
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
   record_type , 
  transaction_code , 
  bank_routing_number , 
  dfi_account_number , 
  amount_billed , 
  aarp_account_number , 
  aarp_association_code , 
  individual_name_last_name , 
  individual_name_first_initial , 
  payment_type_code , 
  discretionary_data , 
  addenda_record_ind , 
  trace_number , 
  sequence_number , 
  process_date ,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL_billing_jpmceftm)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, null, NULL, NULL);                                 


V_STEP := ''STEP5'';

V_STEP_NAME := ''billing_noc''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

V_QUERY9 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL_billing_noc||'' AS 
(SELECT      process_date , 
  batch_number , 
  item_sequence_number , 
  new_bank_account_number , 
  new_bank_routing_number , 
  membership_number , 
  aarp_association_code , 
  new_bank_account_type , 
  noc_change_code , 
  original_bank_account_number , 
  original_bank_routing_number , 
  original_bank_account_type
FROM ''||:V_SRC_TBL_billing_noc||'' WHERE 1 = 2); ''; 

V_QUERY10 := ''  
COPY INTO ''||:V_TEMP_TBL_billing_noc||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
,$9 
,$10 
,$11 
,$12
from  ''||:STAGE||''/billing_noc)
FILE_FORMAT = ''''UTIL.FF_PIPE_CSV'''';''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY9;  
execute immediate :V_QUERY10; 


INSERT INTO IDENTIFIER(:V_SRC_TBL_billing_noc)
(
  process_date , 
  batch_number , 
  item_sequence_number , 
  new_bank_account_number , 
  new_bank_routing_number , 
  membership_number , 
  aarp_association_code , 
  new_bank_account_type , 
  noc_change_code , 
  original_bank_account_number , 
  original_bank_routing_number , 
  original_bank_account_type,
  ISDC_CREATED_DT,	
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  process_date , 
  batch_number , 
  item_sequence_number , 
  new_bank_account_number , 
  new_bank_routing_number , 
  membership_number , 
  aarp_association_code , 
  new_bank_account_type , 
  noc_change_code , 
  original_bank_account_number , 
  original_bank_routing_number , 
  original_bank_account_type,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL_billing_noc)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, null, NULL, NULL);                                 


V_STEP := ''STEP6'';

V_STEP_NAME := ''billing_pitts''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

V_QUERY11 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL_billing_pitts||'' AS 
(SELECT        batch_type , 
  process_julian_date , 
  batch_number , 
  sequence_number , 
  transaction_code , 
  membership_number , 
  association_code , 
  coupon_date , 
  type_indicator, 
  check_number , 
  bank_account_routing_number , 
  employer_premium , 
  bank_account_number , 
  premium_amount , 
  membership_dues , 
  andrus_donation , 
  total_dollar_amount ,
  process_date 
FROM ''||:V_SRC_TBL_billing_pitts||'' WHERE 1 = 2); ''; 

V_QUERY12 := ''  
COPY INTO ''||:V_TEMP_TBL_billing_pitts||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
,$9 
,$10 
,$11 
,$12
,$13 
,$14
,$15
,$16
,$17
,$18
from  ''||:STAGE||''/billing_pitts)
FILE_FORMAT = ''''UTIL.FF_PIPE_CSV'''';''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY11;  
execute immediate :V_QUERY12; 


INSERT INTO IDENTIFIER(:V_SRC_TBL_billing_pitts)
(
  batch_type , 
  process_julian_date , 
  batch_number , 
  sequence_number , 
  transaction_code , 
  membership_number , 
  association_code , 
  coupon_date , 
  type_indicator, 
  check_number , 
  bank_account_routing_number , 
  employer_premium , 
  bank_account_number , 
  premium_amount , 
  membership_dues , 
  andrus_donation , 
  total_dollar_amount ,
  process_date ,
  ISDC_CREATED_DT,	
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  batch_type , 
  process_julian_date , 
  batch_number , 
  sequence_number , 
  transaction_code , 
  membership_number , 
  association_code , 
  coupon_date , 
  type_indicator, 
  check_number , 
  bank_account_routing_number , 
  employer_premium , 
  bank_account_number , 
  premium_amount , 
  membership_dues , 
  andrus_donation , 
  total_dollar_amount ,
  process_date ,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL_billing_pitts)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, null, NULL, NULL);                                 


V_STEP := ''STEP7'';

V_STEP_NAME := ''billing_prot''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

V_QUERY13 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL_billing_prot||'' AS 
(SELECT       
  operator_code , 
  transaction_date , 
  time_batch , 
  transaction_code , 
  last_maintenance_date , 
  last_maintenance_clerk , 
  individual , 
  aarp_account_number , 
  aarp_association_code , 
  sub_trans_code , 
  check_date , 
  total_amount , 
  premium_paid , 
  reason_code 
FROM ''||:V_SRC_TBL_billing_prot||'' WHERE 1 = 2); ''; 

V_QUERY14 := ''  
COPY INTO ''||:V_TEMP_TBL_billing_prot||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
,$9 
,$10 
,$11 
,$12
,$13 
,$14
from  ''||:STAGE||''/billing_prot)
FILE_FORMAT = ''''UTIL.FF_PIPE_CSV'''';''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY13;  
execute immediate :V_QUERY14; 


INSERT INTO IDENTIFIER(:V_SRC_TBL_billing_prot)
(
  operator_code , 
  transaction_date , 
  time_batch , 
  transaction_code , 
  last_maintenance_date , 
  last_maintenance_clerk , 
  individual , 
  aarp_account_number , 
  aarp_association_code , 
  sub_trans_code , 
  check_date , 
  total_amount , 
  premium_paid , 
  reason_code ,
  ISDC_CREATED_DT,	
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  operator_code , 
  transaction_date , 
  time_batch , 
  transaction_code , 
  last_maintenance_date , 
  last_maintenance_clerk , 
  individual , 
  aarp_account_number , 
  aarp_association_code , 
  sub_trans_code , 
  check_date , 
  total_amount , 
  premium_paid , 
  reason_code ,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL_billing_prot)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, null, NULL, NULL);                                 


V_STEP := ''STEP8'';

V_STEP_NAME := ''billing_thdparty''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

V_QUERY15 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL_billing_thdparty||'' AS 
(SELECT       
  record_type_code , 
  transaction_code , 
  receiving_dfi_identification , 
  check_digit , 
  dfi_account_number , 
  dollar_amount , 
  individual_identifcation_number , 
  individual_name , 
  discretionary_data , 
  addenda_record_indicator , 
  trace_number , 
  process_date 
FROM ''||:V_SRC_TBL_billing_thdparty||'' WHERE 1 = 2); ''; 

V_QUERY16 := ''  
COPY INTO ''||:V_TEMP_TBL_billing_thdparty||'' FROM  (
select 
 $1 
,$2 
,$3 
,$4 
,$5 
,$6 
,$7 
,$8 
,$9 
,$10 
,$11 
,$12
from  ''||:STAGE||''/billing_thdparty)
FILE_FORMAT = ''''UTIL.FF_PIPE_CSV'''';''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY15;  
execute immediate :V_QUERY16; 


INSERT INTO IDENTIFIER(:V_SRC_TBL_billing_thdparty)
(
  record_type_code , 
  transaction_code , 
  receiving_dfi_identification , 
  check_digit , 
  dfi_account_number , 
  dollar_amount , 
  individual_identifcation_number , 
  individual_name , 
  discretionary_data , 
  addenda_record_indicator , 
  trace_number , 
  process_date ,
  ISDC_CREATED_DT,	
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  record_type_code , 
  transaction_code , 
  receiving_dfi_identification , 
  check_digit , 
  dfi_account_number , 
  dollar_amount , 
  individual_identifcation_number , 
  individual_name , 
  discretionary_data , 
  addenda_record_indicator , 
  trace_number , 
  process_date ,
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL_billing_thdparty)
;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, null, NULL, NULL);                                 



EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
