USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE "SP_FWA_PROVIDERSPIKE"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    
    V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());
    V_prof_claim_part                   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_claim_part'';
    V_prof_subscriber                   VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_subscriber'';
    V_prof_provider                     VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_provider'';
    V_prof_provider_all                 VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_provider_all'';
    V_provider_spike_raw                VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.provider_spike_raw'';
    V_TMP_PROVIDER_SPIKE                VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PROVIDER_SPIKE'';
    V_TMP_PROVIDER_SPIKE_DATE_PIVOT     VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PROVIDER_SPIKE_DATE_PIVOT'';
    V_TMP_PROVDER_SPIKE_FINAL_OUTPUT    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PROVDER_SPIKE_FINAL_OUTPUT'';
    V_SP_PROCESS_RUN_LOGS_DTL           VARCHAR     := :DB_NAME ||  ''.'' ||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
    V_PROCESS_NAME                      VARCHAR         DEFAULT ''FWA_PROVIDERSPIKE'';
    V_SUB_PROCESS_NAME                  VARCHAR         DEFAULT ''FWA_PROVIDERSPIKE'';
    V_STEP                              VARCHAR;
    V_STEP_NAME                         VARCHAR;
    V_START_TIME                        VARCHAR;
    V_END_TIME                          VARCHAR;
    V_ROWS_LOADED                      INTEGER;
    V_LAST_QUERY_ID                     VARCHAR;
    V_STAGE_QUERY                       VARCHAR;
    V_ROWS_PARSED                       INTEGER;
    V_MESSAGE                           VARCHAR;

BEGIN

   ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD PROVIDER_SPIKE_RAW'';
   
  V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

INSERT INTO
    identifier(:V_provider_spike_raw)
    (
    SERVICE_DATE ,
	SUBSCRIBER_ID ,
	PROVIDERNPI ,
	NPISOURCE ,
	BILLPROVIDERTAXID ,
	BILLPROVIDERSTATES ,
	MONTHS ,
	YEARS ,
	CLAIM_ID ,
	LINE_ITEM_CHARGE_AMT ,
	BILL_LINE_NUM ,
	GRP_CONTROL_NO ,
	TRANCACTSET_CNTL_NO ,
	PROVIDER_HL_NO ,
	SUBSCRIBER_HL_NO ,
	PAYER_HL_NO ,
	TRANSACTSET_CREATE_DATE
    
    )
    
    
with claims AS (SELECT DISTINCT split_part(service_date, ''-'',1) service_date, 
SUBSTR(transactset_create_date,5,2) as months, 
SUBSTR(transactset_create_date,1,4) as years, grp_control_no, trancactset_cntl_no, transactset_create_date, claim_id, line_item_charge_amt, sl_seq_num, provider_hl_no, subscriber_hl_no, payer_hl_no, xml_md5
FROM identifier(:V_prof_claim_part)
where transactset_create_date between (select max(transactset_create_date)+1 from identifier(:V_provider_spike_raw)) and (select TO_VARCHAR(LAST_DAY(DATEADD(MONTH,-1,:V_CURRENT_DATE)),''YYYYMMDD''))
)
,
subscribers AS (SELECT DISTINCT subscriber_id, grp_control_no, trancactset_cntl_no, transactset_create_date, subscriber_hl_no, xml_md5
FROM identifier(:V_prof_subscriber)
where transactset_create_date between (select max(transactset_create_date)+1 from identifier(:V_provider_spike_raw)) and (select TO_VARCHAR(LAST_DAY(DATEADD(MONTH,-1,:V_CURRENT_DATE)),''YYYYMMDD''))
)
,
billProvider AS (
SELECT DISTINCT grp_control_no, trancactset_cntl_no, transactset_create_date, provider_hl_no, provider_id, provider_tax_id, provider_state, xml_md5
FROM identifier(:V_prof_provider)
where transactset_create_date between (select max(transactset_create_date)+1 from identifier(:V_provider_spike_raw)) and (select TO_VARCHAR(LAST_DAY(DATEADD(MONTH,-1,:V_CURRENT_DATE)),''YYYYMMDD''))
)
,
 provider AS (SELECT DISTINCT grp_control_no, trancactset_cntl_no, transactset_create_date, provider_hl_no, clm_rendering_prv_map, subscriber_hl_no, payer_hl_no, claim_id
  FROM identifier(:V_prof_provider_all)
  where transactset_create_date between (select max(transactset_create_date)+1 from identifier(:V_provider_spike_raw)) and (select TO_VARCHAR(LAST_DAY(DATEADD(MONTH,-1,:V_CURRENT_DATE)),''YYYYMMDD''))
)


SELECT DISTINCT service_date, subscriber_id,
  CASE
  WHEN length(clm_rendering_prv_map[''prv_id'']) > 0
          THEN clm_rendering_prv_map[''prv_id'']
   ELSE provider_id
  END as providerNPI,
  CASE
  WHEN length(clm_rendering_prv_map[''prv_id'']) > 0
          THEN ''rendering''
        ELSE ''billing''
  END as NPISource,
  provider_tax_id as billProvidertaxID, provider_state as billProviderStates, months, years,
  claims.claim_id AS claim_id, claims.line_item_charge_amt AS line_item_charge_amt, claims.sl_seq_num AS sl_seq_num,
  claims.grp_control_no AS grp_control_no, claims.trancactset_cntl_no AS trancactset_cntl_no,claims.provider_hl_no AS provider_hl_no,
  claims.subscriber_hl_no AS subscriber_hl_no, claims.payer_hl_no AS payer_hl_no, claims.transactset_create_date AS transactset_create_date
  FROM claims claims
  JOIN subscribers members
    ON (claims.transactset_create_date = members.transactset_create_date
    AND claims.grp_control_no = members.grp_control_no
    AND claims.trancactset_cntl_no = members.trancactset_cntl_no
    AND claims.subscriber_hl_no = members.subscriber_hl_no
    AND claims.xml_md5 = members.xml_md5)
										 
  JOIN billProvider billProvider
    ON (claims.transactset_create_date=billProvider.transactset_create_date
    AND claims.grp_control_no=billProvider.grp_control_no
    AND claims.trancactset_cntl_no=billProvider.trancactset_cntl_no
    AND claims.provider_hl_no = billProvider.provider_hl_no
    AND claims.xml_md5 = billProvider.xml_md5)
											  
  LEFT JOIN provider provider
    ON (claims.transactset_create_date=provider.transactset_create_date
    AND  claims.grp_control_no=provider.grp_control_no
    AND claims.trancactset_cntl_no=provider.trancactset_cntl_no
    AND claims.provider_hl_no  = provider.provider_hl_no
    AND claims.subscriber_hl_no = provider.subscriber_hl_no
    AND claims.payer_hl_no = provider.payer_hl_no
    AND claims.claim_id = provider.claim_id)
    ;
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_provider_spike_raw)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
                                                                                                                           
        
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC,  ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
  
  V_STEP := ''STEP2'';
   
   V_STEP_NAME := ''LOAD TMP_PROVIDER_SPIKE'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                            

  
create or replace temporary table identifier(:V_TMP_PROVIDER_SPIKE)
AS (
select t1.providerNPI,t1.NPISource,t1.billProvidertaxID,t1.billProviderStates,yearmonth,''Scenario 2 - Average Daily Patient Visits'' as scenario,dailypatient_count as metric_values,''<15'' as expectedValues from (

SELECT providerNPI, NPISource, billProvidertaxID, billProviderStates, concat(years,''/'', months) yearmonth, AVG(counts) as dailypatient_count
FROM  (SELECT providerNPI, NPISource, billProvidertaxID, billProviderStates, service_date, COUNT(DISTINCT subscriber_id) counts, SUBSTR(service_date,1,4) as years, 
        SUBSTR(service_date,5,2) as months
        FROM IDENTIFIER(:V_provider_spike_raw)
        WHERE transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'')
        AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-13,:V_CURRENT_DATE))),''YYYYMMDD'')
        AND service_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-13,:V_CURRENT_DATE))),''YYYYMMDD'')
        GROUP BY providerNPI, NPISource, billProvidertaxID, billProviderStates, service_date) daily
GROUP BY providerNPI, NPISource, billProvidertaxID, billProviderStates, concat(years,''/'', months)
) t1
JOIN (
SELECT providerNPI, NPISource, billProvidertaxID, billProviderStates, service_date, COUNT(DISTINCT subscriber_id) counts, SUBSTR(service_date,1,4) as years, 
        SUBSTR(service_date,5,2) as months
        FROM IDENTIFIER(:V_provider_spike_raw)
        WHERE transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'')
        AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-13,:V_CURRENT_DATE))),''YYYYMMDD'')
        AND service_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-1,:V_CURRENT_DATE))),''YYYYMMDD'')
        GROUP BY providerNPI, NPISource, billProvidertaxID, billProviderStates, service_date
        having COUNT(DISTINCT subscriber_id) > 15
        ) t2
   
on t1.providerNPI = t2.providerNPI and t1.NPISource = t2.NPISource and t1.billProvidertaxID = t2.billProvidertaxID and t1.billProviderStates = t2.billProviderStates

UNION ALL

select t1.providerNPI,t1.NPISource,t1.billProvidertaxID,t1.billProviderStates,yearmonth,''Scenario 3 - Dollar Amount'' as scenario,amounts,cast((total_amounts/12) as string) expected_values from (
select providerNPI,NPISource,billProvidertaxID,billProviderStates,yearmonth,amounts, sum(line_item_charge_amt) over(partition by providerNPI,NPISource,billProvidertaxID,billProviderStates) total_amounts

from (
SELECT providerNPI, NPISource, billProvidertaxID, billProviderStates, concat(years,''/'', months) yearmonth,
SUM(line_item_charge_amt) as amounts, 
sum(case when transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'') AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-12,:V_CURRENT_DATE))),''YYYYMMDD'') THEN line_item_charge_amt end) line_item_charge_amt
FROM IDENTIFIER(:V_provider_spike_raw)
WHERE transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'')
AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-13,:V_CURRENT_DATE))),''YYYYMMDD'')
GROUP BY providerNPI, NPISource, billProvidertaxID, billProviderStates, concat(years,''/'', months)
) t
) t1
join (

SELECT providerNPI, NPISource, billProvidertaxID, billProviderStates, 
SUM(line_item_charge_amt) as last_month_amount
FROM IDENTIFIER(:V_provider_spike_raw)
WHERE transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'')
AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-1,:V_CURRENT_DATE))),''YYYYMMDD'')
GROUP BY providerNPI, NPISource, billProvidertaxID, billProviderStates
) t2
on t1.providerNPI = t2.providerNPI and t1.NPISource = t2.NPISource and t1.billProvidertaxID = t2.billProvidertaxID and t1.billProviderStates = t2.billProviderStates
where last_month_amount> (total_amounts/12)*1.5


UNION ALL

select t1.providerNPI,t1.NPISource,t1.billProvidertaxID,t1.billProviderStates,yearmonth,''Scenario 1 - Claim Count'' as scenario,number,cast((total_number1/12) as string) expected_values from (
select providerNPI,NPISource,billProvidertaxID,billProviderStates,yearmonth,number, sum(total_number) over(partition by providerNPI,NPISource,billProvidertaxID,billProviderStates) total_number1

from (
SELECT providerNPI, NPISource, billProvidertaxID, billProviderStates, concat(years,''/'', months) yearmonth,
COUNT(DISTINCT transactset_create_date, grp_control_no, trancactset_cntl_no, provider_hl_no, subscriber_hl_no, payer_hl_no, claim_id) as number, 
count(distinct case when transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'') AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-12,:V_CURRENT_DATE))),''YYYYMMDD'') THEN (transactset_create_date || grp_control_no || trancactset_cntl_no || provider_hl_no ||subscriber_hl_no || payer_hl_no || claim_id) end) total_number
FROM IDENTIFIER(:V_provider_spike_raw)
WHERE transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'')
AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-13,:V_CURRENT_DATE))),''YYYYMMDD'')
GROUP BY providerNPI, NPISource, billProvidertaxID, billProviderStates, concat(years,''/'', months)
) t
) t1
join (

SELECT providerNPI, NPISource, billProvidertaxID, billProviderStates, 
COUNT(DISTINCT transactset_create_date, grp_control_no, trancactset_cntl_no, provider_hl_no, subscriber_hl_no, payer_hl_no, claim_id) as last_month_number
FROM IDENTIFIER(:V_provider_spike_raw)
WHERE transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'')
AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-1,:V_CURRENT_DATE))),''YYYYMMDD'')
GROUP BY providerNPI, NPISource, billProvidertaxID, billProviderStates
) t2
on t1.providerNPI = t2.providerNPI and t1.NPISource = t2.NPISource and t1.billProvidertaxID = t2.billProvidertaxID and t1.billProviderStates = t2.billProviderStates
where last_month_number > (total_number1/12)*1.5
);

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PROVIDER_SPIKE)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                              :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


   V_STEP := ''STEP3'';
   
   V_STEP_NAME := ''LOAD TMP_PROVIDER_SPIKE_DATE_PIVOT'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
select ltrim(t1.providerNPI,''0#'') as "providerNPI", t1.NPISource as "npisource", ltrim(t1.billProvidertaxID,''0#'') as "billproviderstates",t1.billProviderStates as "billprovidertaxid",t1.scenario as "scenario",t1.yearmonth as pivot_column,t1.expectedvalues as "expectedValues",zeroifnull(t2.metric_values) pivot_value from (
select distinct providerNPI, NPISource, billProvidertaxID, billProviderStates,scenario,b.yearmonth,expectedvalues from identifier(:V_TMP_PROVIDER_SPIKE) a
cross join (
select distinct concat(years,''/'',months) yearmonth
FROM IDENTIFIER(:V_provider_spike_raw)
WHERE transactset_create_date < TO_VARCHAR(DATE_TRUNC(MONTH,:V_CURRENT_DATE),''YYYYMMDD'')
AND transactset_create_date >= TO_VARCHAR(DATE_TRUNC(MONTH,(DATEADD(MONTH,-13,:V_CURRENT_DATE))),''YYYYMMDD'')
) b 
) t1
left outer join identifier(:V_TMP_PROVIDER_SPIKE) t2 
on t1.providerNPI = t2.providerNPI and t1.NPISource = t2.NPISource and t1.billProvidertaxID = t2.billProvidertaxID and t1.billProviderStates = t2.billProviderStates and t1.scenario=t2.scenario
and t1.yearmonth = t2.yearmonth
;


call src_edi_837.sp_Provider_Spike_pivot_prev_results();

create or replace temporary table identifier(:V_TMP_PROVDER_SPIKE_FINAL_OUTPUT)
AS (
select * from table(result_scan(last_query_id(-2))));

  V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PROVDER_SPIKE_FINAL_OUTPUT)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                              :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

V_STEP := ''STEP4'';

 
V_STEP_NAME := ''Generate File for fwd Provider Spike''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());   



V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_providerspike/''||''edp_providerspike_monthly_''||(select TO_VARCHAR(:V_CURRENT_DATE,''YYYYMM''))||''.csv''||'' FROM (
             
             
             
             SELECT *
               FROM TMP_PROVDER_SPIKE_FINAL_OUTPUT
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

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC,  ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);

RAISE;

END;

';
