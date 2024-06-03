
USE SCHEMA SRC_COMPAS;
CREATE OR REPLACE PROCEDURE SP_COMPAS_BILLING_LOAD("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "LZ_COMPAS_SC" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''COMPAS_BILLING'';

V_SUB_PROCESS_NAME  VARCHAR ;

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;



V_BILLING_DALLAS            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.BILLING_DALLAS'';
V_BILLING_EFTAUTH           VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.BILLING_EFTAUTH'';
V_BILLING_JPMCEFTD          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.BILLING_JPMCEFTD'';
V_BILLING_JPMCEFTM          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.BILLING_JPMCEFTM'';
V_BILLING_NOC               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.BILLING_NOC'';
V_BILLING_PITTS             VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.BILLING_PITTS'';
V_BILLING_PROT              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.BILLING_PROT'';
V_BILLING_THDPARTY          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_COMPAS'') || ''.BILLING_THDPARTY'';


    
V_COMPAS_BILLING_RAW_DALLAS            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.COMPAS_BILLING_RAW_DALLAS'';
V_COMPAS_BILLING_RAW_EFTAUTH           VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.COMPAS_BILLING_RAW_EFTAUTH'';
V_COMPAS_BILLING_RAW_JPMCEFTD          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.COMPAS_BILLING_RAW_JPMCEFTD'';
V_COMPAS_BILLING_RAW_JPMCEFTM          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.COMPAS_BILLING_RAW_JPMCEFTM'';
V_COMPAS_BILLING_RAW_NOC               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.COMPAS_BILLING_RAW_NOC'';
V_COMPAS_BILLING_RAW_PITTS             VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.COMPAS_BILLING_RAW_PITTS'';
V_COMPAS_BILLING_RAW_PROT              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.COMPAS_BILLING_RAW_PROT'';
V_COMPAS_BILLING_RAW_THDPARTY          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_COMPAS_SC,''LZ_COMPAS'') || ''.COMPAS_BILLING_RAW_THDPARTY'';





BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
   
V_SUB_PROCESS_NAME :=  ''COMPAS_BILLING_LOAD'';
   
V_STEP_NAME := ''LOAD BILLING_DALLAS''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
    
INSERT INTO IDENTIFIER(:V_BILLING_DALLAS) 
(BATCH_TYPE        ,          
PROCESS_JULIAN_DATE,         
BATCH_NUMBER       ,         
SEQUENCE_NUMBER    ,         
TRANSACTION_CODE   ,         
MEMBERSHIP_NUMBER  ,         
ASSOCIATION_CODE   ,         
COUPON_DATE        ,         
TYPE_INDICATOR     ,         
CHECK_NUMBER       ,         
BANK_ACCOUNT_ROUTING_NUMBER ,
EMPLOYER_PREMIUM   ,         
BANK_ACCOUNT_NUMBER ,        
PREMIUM_AMOUNT      ,       
MEMBERSHIP_DUES     ,        
ANDRUS_DONATION     ,        
TOTAL_DOLLAR_AMOUNT ,		
PROCESS_DATE 		,		
ISDC_CREATED_DT 	,
ISDC_UPDATED_DT			
)
SELECT
substr(text,1,4) as Batch_Type,
substr(text,5,5) as Process_julian_Date,
substr(text,10,6) as Batch_Number,
substr(text,16,3) as Sequence_Number,
CAST(substr(text,19,2) as INT) as Transaction_Code,
substr(text,21,9) as Membership_Number,
CAST(substr(text,30,1) as INT) as Association_Code,
substr(text,38,6) as Coupon_Date,
CAST(substr(text,80,1) as VARCHAR(1)) as Type_Indicator,
substr(text,81,20) as Check_Number,
substr(text,115,9) as Bank_Account_Routing_Number,
substr(text,135,8) as Employer_Premium,
substr(text,143,17) as Bank_Account_Number,
CAST(substr(text,160,10) as INT) as Premium_Amount,
CAST(substr(text,170,4) as INT) as Membership_Dues,
CAST(substr(text,174,5) as INT) as Andrus_Donation,
CAST(REGEXP_REPLACE(substr(text,179,12), ''^0+'', '''') as INT) as Total_Dollar_Amount,
to_char(current_date,''MMddyy'') AS Process_Date,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP()
FROM IDENTIFIER(:V_COMPAS_BILLING_RAW_DALLAS) WHERE text like ''SMMU%''; 


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
V_STEP := ''STEP2'';
   
V_SUB_PROCESS_NAME :=  ''COMPAS_BILLING_LOAD'';
   
V_STEP_NAME := ''LOAD BILLING_EFTAUTH''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
    
INSERT INTO IDENTIFIER(:V_BILLING_EFTAUTH) 
(PROCESS_DATE ,
	BATCH_NUMBER ,
	ITEM_SEQUENCE_NUMBER ,
	BANK_ACCOUNT_NUMBER ,
	BANK_ROUTING_NUMBER ,
	AARP_ACCOUNT_NUMBER,
	AARP_ASSOCIATION_CODE ,
	BANK_ACCOUNT_TYPE,
    ISDC_CREATED_DT 	,
    ISDC_UPDATED_DT			
)
select
substr(text,1,6) as Process_Date,
CAST(substr(text,7,6) as INT) as Batch_Number,
substr(text,13,4) as Item_Sequence_Number,
ltrim(substr(text,26,17)) as Bank_Account_Number,
substr(text,43,9) as Bank_Routing_Number,
CAST(substr(text,52,9) as INT) as AARP_Account_Number,
CAST(substr(text,61,1) as VARCHAR(1)) as AARP_Association_Code,
CAST(substr(text,62,1) as VARCHAR(1)) as Bank_Account_Type,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP() 
from IDENTIFIER(:V_COMPAS_BILLING_RAW_EFTAUTH) where text not like ''T%EFTAUTH%''; 


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
V_STEP := ''STEP3'';
   
V_SUB_PROCESS_NAME :=  ''COMPAS_BILLING_LOAD'';
   
V_STEP_NAME := ''LOAD BILLING_JPMCEFTD''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
    
INSERT INTO IDENTIFIER(:V_BILLING_JPMCEFTD)(
RECORD_TYPE 				,
TRANSACTION_CODE 			,
BANK_ROUTING_NUMBER 		,
DFI_ACCOUNT_NUMBER 			,
AMOUNT_BILLED 				,
AARP_ACCOUNT_NUMBER 		,
AARP_ASSOCIATION_CODE 		,
INDIVIDUAL_NAME_LAST_NAME 	,
INDIVIDUAL_NAME_FIRST_INITIAL,
PAYMENT_TYPE_CODE 			,
DISCRETIONARY_DATA 			,
ADDENDA_RECORD_IND 			,
TRACE_NUMBER 				,
SEQUENCE_NUMBER 			,
PROCESS_DATE 				,
ISDC_CREATED_DT 	,
ISDC_UPDATED_DT			
 				
)
SELECT
CAST(substr(text,1,1) as VARCHAR(1)) as Record_Type,
CAST(substr(text,2,2) as VARCHAR(2)) as Transaction_Code,
substr(text,4,9) as Bank_Routing_Number,
substr(text,13,17) as DFI_Account_Number,
CAST(nullif(substr(text,30,10),'''') as INT) as Amount_Billed,
substr(text,40,9) as AARP_Account_Number,
CAST(nullif(substr(text,49,1),'''') as INT) as AARP_Association_Code,
ltrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(substr(text,55,21), ''\\[?#=()_:.,*]'', '' ''), ''\\\\['', '' ''), '']'', '' ''), ''/'', '' '')) as Individual_Name_Last_Name,
regexp_replace(regexp_replace(regexp_replace(substr(text,76,1), ''\\[?#=()_:.,*]'', ''\\ ''), ''\\\\['', '' ''), ''\\\\]'', '' '') as Individual_Name_First_Initial,
CAST(substr(text,77,1) as VARCHAR(1)) as Payment_Type_Code,
CAST(substr(text,78,1) as VARCHAR(1)) as Discretionary_Data,
substr(text,79,1) as Addenda_Record_Ind,
substr(text,80,8) as Trace_Number,
substr(text,88,7) as Sequence_Number,
to_char(current_date,''MMddyy'') AS Process_Date,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP() 
from IDENTIFIER(:V_COMPAS_BILLING_RAW_JPMCEFTD)  where text  like ''6%'';


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

 V_STEP := ''STEP4'';
   
V_SUB_PROCESS_NAME :=  ''COMPAS_BILLING_LOAD'';
   
V_STEP_NAME := ''LOAD BILLING_JPMCEFTM''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
    
INSERT INTO IDENTIFIER(:V_BILLING_JPMCEFTM)
(
RECORD_TYPE 				,	
TRANSACTION_CODE 			,	
BANK_ROUTING_NUMBER 		,	
DFI_ACCOUNT_NUMBER 			,	
AMOUNT_BILLED 				,	
AARP_ACCOUNT_NUMBER 		,	
AARP_ASSOCIATION_CODE 		,	
INDIVIDUAL_NAME_LAST_NAME 	,	
INDIVIDUAL_NAME_FIRST_INITIAL, 	
PAYMENT_TYPE_CODE 			,	
DISCRETIONARY_DATA 			,	
ADDENDA_RECORD_IND 			,	
TRACE_NUMBER 				,	
SEQUENCE_NUMBER 			,	
PROCESS_DATE 				,	
ISDC_CREATED_DT 	,
ISDC_UPDATED_DT			
				

)
SELECT
CAST(substr(text,1,1) as VARCHAR(1)) as Record_Type,
CAST(substr(text,2,2) as VARCHAR(2)) as Transaction_Code,
substr(text,4,9) as Bank_Routing_Number,
substr(text,13,17) as DFI_Account_Number,
CAST(nullif(substr(text,30,10),'''') as INT) as Amount_Billed,
substr(text,40,9) as AARP_Account_Number,
CAST(nullif(substr(text,49,1),'''') as INT) as AARP_Association_Code,
ltrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(substr(text,55,21), ''\\[?#=()_:.,*]'', '' ''), ''\\\\['', '' ''), '']'', '' ''), ''/'', '' ''))  as Individual_Name_Last_Name,
regexp_replace(regexp_replace(regexp_replace(substr(text,76,1),''\\[?#=()_:.,*]'', '' ''), ''\\\\['', '' ''), ''\\\\]'', '' '') as Individual_Name_First_Initial,
CAST(substr(text,77,1) as VARCHAR(1)) as Payment_Type_Code,
CAST(substr(text,78,1) as VARCHAR(1)) as Discretionary_Data,
substr(text,79,1) as Addenda_Record_Ind,
substr(text,80,8) as Trace_Number,
substr(text,88,7) as Sequence_Number,
to_char(current_date,''MMddyy'') AS Process_Date,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP() 
from IDENTIFIER(:V_COMPAS_BILLING_RAW_JPMCEFTM)  where text  like ''6%'';




V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
 V_STEP := ''STEP5'';
   
V_SUB_PROCESS_NAME :=  ''COMPAS_BILLING_LOAD'';
   
V_STEP_NAME := ''LOAD BILLING_NOC''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
    
INSERT INTO IDENTIFIER(:V_BILLING_NOC)
(PROCESS_DATE ,
	BATCH_NUMBER ,
	ITEM_SEQUENCE_NUMBER ,
	NEW_BANK_ACCOUNT_NUMBER,
	NEW_BANK_ROUTING_NUMBER ,
	MEMBERSHIP_NUMBER ,
	AARP_ASSOCIATION_CODE ,
	NEW_BANK_ACCOUNT_TYPE ,
	NOC_CHANGE_CODE ,
	ORIGINAL_BANK_ACCOUNT_NUMBER ,
	ORIGINAL_BANK_ROUTING_NUMBER ,
	ORIGINAL_BANK_ACCOUNT_TYPE,
    ISDC_CREATED_DT 	,
ISDC_UPDATED_DT			
)
SELECT
substr(text,1,6) as Process_Date,
substr(text,7,6) as Batch_Number,
substr(text,13,4) as Item_Sequence_Number,
substr(text,17,17) as New_Bank_Account_Number,
substr(text,34,9) as New_Bank_Routing_Number,
substr(text,43,9) as Membership_Number,
substr(text,52,1) as AARP_Association_Code,
substr(text,53,1) as New_Bank_Account_type,
substr(text,54,3) as NOC_Change_Code,
substr(text,57,17) as Original_Bank_Account_Number,
substr(text,74,9) as Original_Bank_Routing_Number,
substr(text,83,1) as Original_Bank_Account_Type,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP() 
from IDENTIFIER(:V_COMPAS_BILLING_RAW_NOC) where text not like ''T%NOTICEOFCHANGE%'';


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
 V_STEP := ''STEP6'';
   
V_SUB_PROCESS_NAME :=  ''COMPAS_BILLING_LOAD'';
   
V_STEP_NAME := ''LOAD BILLING_PITTS''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
    
INSERT INTO IDENTIFIER(:V_BILLING_PITTS) (
BATCH_TYPE 					,
PROCESS_JULIAN_DATE 		,
BATCH_NUMBER 				,
SEQUENCE_NUMBER 			,
TRANSACTION_CODE 			,
MEMBERSHIP_NUMBER 			,
ASSOCIATION_CODE 			,
COUPON_DATE 				,
TYPE_INDICATOR				,
CHECK_NUMBER 				,
BANK_ACCOUNT_ROUTING_NUMBER ,
EMPLOYER_PREMIUM 			,
BANK_ACCOUNT_NUMBER 		,
PREMIUM_AMOUNT 				,
MEMBERSHIP_DUES 			,
ANDRUS_DONATION 			,
TOTAL_DOLLAR_AMOUNT 		,
PROCESS_DATE 				,
ISDC_CREATED_DT 	,
ISDC_UPDATED_DT			
 				
)
SELECT
substr(text,1,4) as Batch_Type,
substr(text,5,5) as Process_Julian_Date,
substr(text,10,6) as Batch_Number,
substr(text,16,3) as Sequence_Number,
CAST(substr(text,19,2) as INT) as Transaction_Code,
substr(text,21,9) as Membership_Number,
CAST(substr(text,30,1) as INT) as Association_Code,
substr(text,38,6) as Coupon_Date,
CAST(substr(text,80,1) as VARCHAR(1)) as Type_Indicator,
substr(text,81,20) as Check_Number,
substr(text,115,9) as Bank_Account_Routing_Number,
substr(text,135,8) as Employer_Premium,
substr(text,143,17) as Bank_Account_Number,
CAST(substr(text,160,10) as INT) as Premium_Amount,
CAST(substr(text,170,4) as INT) as Membership_Dues,
CAST(substr(text,174,5) as INT) as Andrus_Donation,
CAST(REGEXP_REPLACE(substr(text,179,12), ''^0+'', '''') as INT) as Total_Dollar_Amount,
to_char(current_date,''MMddyy'') AS Process_Date,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP() 
FROM IDENTIFIER(:V_COMPAS_BILLING_RAW_PITTS) WHERE text like ''SMMU%'' and  Type_Indicator = ''P''; 


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


 V_STEP := ''STEP7'';
   
V_SUB_PROCESS_NAME :=  ''COMPAS_BILLING_LOAD'';
   
V_STEP_NAME := ''LOAD BILLING_PROT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
    
INSERT INTO IDENTIFIER(:V_BILLING_PROT)
(OPERATOR_CODE ,
	TRANSACTION_DATE ,
	TIME_BATCH,
	TRANSACTION_CODE,
	LAST_MAINTENANCE_DATE,
	LAST_MAINTENANCE_CLERK ,
	INDIVIDUAL,
	AARP_ACCOUNT_NUMBER ,
	AARP_ASSOCIATION_CODE,
	SUB_TRANS_CODE ,
	CHECK_DATE ,
	TOTAL_AMOUNT ,
	PREMIUM_PAID,
	REASON_CODE,
    ISDC_CREATED_DT 	,
ISDC_UPDATED_DT			
)
select
substr(text,1,4) as Operator_Code,
substr(text,5,5) as Transaction_Date,
substr(text,10,9) as Time_Batch,
substr(text,19,2) as Transaction_Code,  
substr(text,21,6) as Last_Maintenance_Date,
substr(text,27,6) as Last_Maintenance_Clerk,
substr(text,33,1) as Individual,
substr(text,34,9) as AARP_Account_Number,
substr(text,43,1) as AARP_Association_Code,
substr(text,44,1) as Sub_Trans_Code,        
substr(text,45,6) as Check_Date,            
--CAST(substr(text,51,12) as int)/100 as Total_Amount,
--CAST(substr(text,63,12)as int)/100 as Premium_Paid,
((substr(text,51,12)::DECIMAL(15,2))/100)::DECIMAL(15,2) as Total_Amount,
((substr(text,63,12)::DECIMAL(15,2))/100)::DECIMAL(15,2) as Premium_Paid,
substr(text,75,3) as Reason_Code,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP() 
 from  IDENTIFIER(:V_COMPAS_BILLING_RAW_PROT) where text not like ''T%EFTPR%'';


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


V_STEP := ''STEP8'';
   
V_SUB_PROCESS_NAME :=  ''COMPAS_BILLING_LOAD'';
   
V_STEP_NAME := ''LOAD BILLING_PROT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
    
INSERT INTO IDENTIFIER(:V_BILLING_THDPARTY) 
(
RECORD_TYPE_CODE 				,
TRANSACTION_CODE 				,
RECEIVING_DFI_IDENTIFICATION 	,
CHECK_DIGIT 					,
DFI_ACCOUNT_NUMBER 				,
DOLLAR_AMOUNT 					,
INDIVIDUAL_IDENTIFCATION_NUMBER ,
INDIVIDUAL_NAME 				,
DISCRETIONARY_DATA 				,
ADDENDA_RECORD_INDICATOR 		,
TRACE_NUMBER					,
PROCESS_DATE 					,
ISDC_CREATED_DT 	,
ISDC_UPDATED_DT			
 					)

SELECT
CAST(substr(text,1,1) as VARCHAR(1)) as Record_Type_Code,
CAST(substr(text,2,2) as VARCHAR(2))  as Transaction_Code,
CAST(substr(text,4,8) as VARCHAR(8)) as Receiving_Dfi_Identification,
substr(text,12,1) as Check_Digit,
substr(text,13,17) as Dfi_Account_Number,
CAST(substr(text,30,10) as INT) as Dollar_Amount,
substr(text,40,15) as Individual_Identifcation_Number,
substr(text,55,22) as Individual_Name,
substr(text,77,2) as Discretionary_Data,
CAST(substr(text,79,1) as INT) as Addenda_Record_Indicator,
substr(text,80,15) as Trace_Number,
to_char(current_date,''MMddyy'') AS Process_Date,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP() 
from IDENTIFIER(:V_COMPAS_BILLING_RAW_THDPARTY) where text  like ''6%'';


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';