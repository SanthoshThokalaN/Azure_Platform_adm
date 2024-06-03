USE SCHEMA SRC_COMPAS;

CREATE OR REPLACE PROCEDURE "SP_EFFECTIVE_ERROR_REPORT_STG"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_COMPAS_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());
V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_MESSAGE                  VARCHAR;
V_PROCESS_NAME             VARCHAR       DEFAULT ''EFFECTIVE_ERROR'';
V_SUB_PROCESS_NAME		   VARCHAR		 DEFAULT ''EFFECTIVE_ERROR_REPORT_STG'';
V_STEP					   VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER;
V_ROWS_LOADED              INTEGER;
V_LAST_QUERY_ID            VARCHAR;
V_TMP_EFFECTIVE_ERROR_STG	        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''LZ_COMPAS'') || ''.TMP_EFFECTIVE_ERROR_STG'';
V_EFFECTIVE_ERROR_REPORT_STG	VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''LZ_COMPAS'') || ''.EFFECTIVE_ERROR_REPORT_STG'';

V_CLG_MSG					        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_COMPAS_SC,''SRC_COMPAS'') || ''.CLG_MSG'';

V_UPDATE_PAYMENT_ID INTEGER := 101022;
V_HOUSE_DEMO_ID  INTEGER := 101006;


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';

V_STEP_NAME := ''Load TMP_EFFECTIVE_ERROR_STG''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
    
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_EFFECTIVE_ERROR_STG) AS 
(
  
SELECT 
REFERENCE_ID
FROM IDENTIFIER(:V_clg_msg)
WHERE MSG_LOG_CREATION_DATE_DL = :V_CURRENT_DATE
AND CONTAINS(TRIM(REGEXP_REPLACE(
COALESCE(ARRAY_TO_STRING(ARRAY_DISTINCT(REGEXP_SUBSTR_ALL(to_varchar(MSG_CLOB),''\\<Description\\>.*+<\\/Description\\>'',1,1)),'',''),''''), ''<Description>|</Description>'', '''')),
''Effective date of change is prior to the Minimum Retroactive Effective Date.'')    

)
;

 
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_EFFECTIVE_ERROR_STG)) ; 

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
 
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                
   V_STEP := ''STEP2'';

   V_STEP_NAME := ''Load EFFECTIVE_ERROR_REPORT_STG''; 
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
 
 TRUNCATE TABLE IDENTIFIER(:V_EFFECTIVE_ERROR_REPORT_STG);
   
INSERT INTO IDENTIFIER(:V_EFFECTIVE_ERROR_REPORT_STG)
(
select
case 
    when up.update_payment_msgtypeid is not null and hd.house_demo_msg_type_id is null then ''BILLING UPDATE'' 
    when up.update_payment_msgtypeid is null and hd.house_demo_msg_type_id is not null then ''ADDRESS UPDATE''
	when up.update_payment_msgtypeid is not null and hd.house_demo_msg_type_id is not null then ''BILLING UPDATE/ADDRESS UPDATE''
	else null
end Failure_Type,
nvl(up.update_payment_individual_id,hd.house_demo_individual_id) as Individual_Id,
up.update_payment_routing_number as Routing_Number,
up.update_payment_account_number as Account_Number,
case
	when hd.house_demo_address_type = ''1'' then ''PERMANENT ADDRESS''
	else null
end Address_Type,
hd.house_demo_address_line1 as Address_Line_1,
hd.house_demo_address_line2 as Address_Line_2,
hd.house_demo_city as City,
hd.house_demo_state_cd as State_CD,
hd.house_demo_zip_code as Zip_CD,
hd.house_demo_country as Country
from (
	select msg_clob,
    regexp_replace(coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<IndividualId\\>[0-9]+<\\/IndividualId\\>'',1,1)),'',''),''''), ''<IndividualId>|</IndividualId>'', '''') as update_payment_individual_id,
     regexp_replace(coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<RoutingNumber\\>.*+<\\/RoutingNumber\\>'',1,1)),'',''),''''), ''<RoutingNumber>|</RoutingNumber>'', '''') as update_payment_routing_number,
         trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<AccountNumber\\>.*<\\/AccountNumber\\>'',1,1)),'',''),''''), ''<AccountNumber>|</AccountNumber>'', '''')) as update_payment_account_number,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<AccountType\\>.*<\\/AccountType\\>'',1,1)),'',''),''''), ''<AccountType>|</AccountType>'', '''')) as update_payment_account_type,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<FirstName\\>.*<\\/FirstName\\>'',1,1)),'',''),''''), ''<FirstName>|</FirstName>'', '''')) as update_payment_first_name,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<MiddleName\\>.*+<\\/MiddleName\\>'',1,1)),'',''),''''), ''<MiddleName>|</MiddleName>'', '''')) as update_payment_middle_name,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<LastName\\>.*+<\\/LastName\\>'',1,1)),'',''),''''), ''<LastName>|</LastName>'', '''')) as update_payment_last_name,
	msg_log_id as update_payment_msg_log_id,
	reference_id as update_payment_referenceid,
	msg_type_id as update_payment_msgtypeid,
	msg_desc as update_payment_msg_desc,
	msg_log_creation_date as update_payment_creation_date
	from IDENTIFIER(:V_clg_msg)
	where 
       len(regexp_replace(coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<RoutingNumber\\>.*+<\\/RoutingNumber\\>'',1,1)),'',''),''''), ''<RoutingNumber>|</RoutingNumber>'', '''')) > 0
    --msg_log_id = 512130543	
	and msg_log_creation_date_dl = :V_CURRENT_DATE
	and msg_type_id = cast(:V_UPDATE_PAYMENT_ID as string) --export update_payment_id=101022 export house_demo_id=101006
	--and size(xpath( to_varchar(msg_clob), ''//RoutingNumber/text()'' )) > 0
	and clg_msg.reference_id in (select reference_id from IDENTIFIER(:V_TMP_EFFECTIVE_ERROR_STG))
) up
full outer join (
	select msg_clob,
    regexp_replace(coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<IndividualId\\>[0-9]+<\\/IndividualId\\>'',1,1)),'',''),''''), ''<IndividualId>|</IndividualId>'', '''') as house_demo_individual_id,
     regexp_replace(coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<AddressType\\>.*+<\\/AddressType\\>'',1,1)),'',''),''''), ''<AddressType>|</AddressType>'', '''') as house_demo_address_type,
         trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<AddressLine1\\>.*<\\/AddressLine1\\>'',1,1)),'',''),''''), ''<AddressLine1>|</AddressLine1>'', '''')) as house_demo_address_line1,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<AddressLine2\\>.*<\\/AddressLine2\\>'',1,1)),'',''),''''), ''<AddressLine2>|</AddressLine2>'', '''')) as house_demo_address_line2,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<City\\>.*+<\\/City\\>'',1,1)),'',''),''''), ''<City>|</City>'', '''')) as house_demo_city,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<State\\>.*+<\\/State\\>'',1,1)),'',''),''''), ''<State>|</State>'', '''')) as house_demo_state_cd,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<ZipCode\\>.*+<\\/ZipCode\\>'',1,1)),'',''),''''), ''<ZipCode>|</ZipCode>'', '''')) as house_demo_zip_code,
trim(regexp_replace(
coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<Country\\>.*+<\\/Country\\>'',1,1)),'',''),''''), ''<Country>|</Country>'', '''')) as house_demo_country,
	msg_log_id as house_demo_msg_log_id,
	reference_id as house_demo_reference_id,
	msg_type_id as house_demo_msg_type_id,
	msg_desc as house_demo_msg_desc,
	msg_log_creation_date as house_demo_creation_date

	from IDENTIFIER(:V_clg_msg)
	where --msg_log_id = 512130543
	msg_log_creation_date_dl = :V_CURRENT_DATE
	and msg_type_id = cast(:V_HOUSE_DEMO_ID as string) --export update_payment_id=101022 export house_demo_id=101006
	and len(regexp_replace(coalesce(array_to_string(array_distinct(regexp_substr_all(to_varchar(msg_clob),''\\<RoutingNumber\\>.*+<\\/RoutingNumber\\>'',1,1)),'',''),''''), ''<RoutingNumber>|</RoutingNumber>'', '''')) > 0
	and clg_msg.reference_id in (select reference_id from IDENTIFIER(:V_TMP_EFFECTIVE_ERROR_STG))
	
) hd
on (up.update_payment_individual_id = hd.house_demo_individual_id)
);

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_EFFECTIVE_ERROR_REPORT_STG)) ;

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
