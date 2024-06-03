USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE SP_REF_PROVIDER_REPORT("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''REF_PROVIDER_REPORT'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''REF_PROVIDER_REPORT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;
V_START_DATE_TRANSACTSET VARCHAR;
V_END_DATE_TRANSACTSET VARCHAR;
V_START_CLM_RECEPT_DT VARCHAR;
V_END_CLM_RECEPT_DT VARCHAR;

V_TMP_REF_PROVIDER_REPORT    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_REF_PROVIDER_REPORT'';

V_PROF_SUBSCRIBER            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_SUBSCRIBER'';
V_PROF_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLAIM_PART'';
V_PROF_PROVIDER              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PROVIDER'';
V_PROF_PROVIDER_ALL          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PROVIDER_ALL'';
V_CH_VIEW                    VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.CH_VIEW'';


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
 
 
V_STEP_NAME := ''Load TMP_REF_PROVIDER_REPORT''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
V_START_DATE_TRANSACTSET := (select TO_VARCHAR(DATEADD(MONTH, -1, :V_CURRENT_DATE)::date,''YYYYMM01''));
V_END_DATE_TRANSACTSET := (select TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, :V_CURRENT_DATE))::date,''YYYYMMDD''));
V_START_CLM_RECEPT_DT :=(select TO_VARCHAR(
                                                                        DATEADD(
                                                                            DAY,
                                                                            -36,
                                                                            :V_CURRENT_DATE)::DATE,''YYYY-MM-DD'') );
																			
V_END_CLM_RECEPT_DT :=(select TO_VARCHAR(
                                                                                DATEADD(
                                                                                    DAY,
                                                                                    5,
                                                                                    :V_CURRENT_DATE)::DATE,''YYYY-MM-DD''));
                                                                                    
CREATE OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_REF_PROVIDER_REPORT)  AS   
WITH claims as
(SELECT DISTINCT
d.clm_recept_dt AS receipt_dt,
clm_num AS claim_no,
clm.sl_seq_num as bill_line_no,
split_part(clm.product_service_id_qlfr,'':'',2) as CPT_codes,
clm.transactset_create_date,
clm.trancactset_cntl_no,
clm.grp_control_no,
clm.payer_hl_no,
clm.provider_hl_no,
clm.subscriber_hl_no,
service_date,
line_item_charge_amt,
total_claim_charge_amt,
vendor_cd,
clm.xml_md5
FROM IDENTIFIER(:V_PROF_CLAIM_PART) clm
        JOIN IDENTIFIER(:V_PROF_SUBSCRIBER) mem ON (mem.grp_control_no = clm.grp_control_no
        AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
        AND mem.subscriber_hl_no = clm.subscriber_hl_no
        AND mem.transactset_create_date = clm.transactset_create_date
        AND mem.xml_md5 = clm.xml_md5)
        JOIN IDENTIFIER(:V_CH_VIEW) d ON clm.payer_clm_ctrl_num = d.medicare_clm_cntrl_num
        AND mem.subscriber_id = d.member_id
        WHERE clm.transactset_create_date BETWEEN :V_START_DATE_TRANSACTSET
    AND :V_END_DATE_TRANSACTSET
        AND d.clm_recept_dt BETWEEN :V_START_CLM_RECEPT_DT
                                                                            AND :V_END_CLM_RECEPT_DT
        AND d.medicare_clm_cntrl_num IS NOT NULL and clm.vendor_cd = ''CMS''
UNION ALL
SELECT DISTINCT
d.clm_recept_dt AS receipt_dt,
clm_num AS claim_no,
clm.sl_seq_num as bill_line_no,
split_part(product_service_id_qlfr,'':'',2) as CPT_codes,
clm.transactset_create_date,
clm.trancactset_cntl_no,
clm.grp_control_no,
clm.payer_hl_no,
clm.provider_hl_no,
clm.subscriber_hl_no,
service_date,
line_item_charge_amt,
total_claim_charge_amt,
vendor_cd,
clm.xml_md5
FROM IDENTIFIER(:V_PROF_CLAIM_PART) clm
        JOIN IDENTIFIER(:V_CH_VIEW) d ON clm.network_trace_number = d.clh_trk_id
        WHERE clm.transactset_create_date BETWEEN :V_START_DATE_TRANSACTSET
    AND :V_END_DATE_TRANSACTSET
        AND d.clm_recept_dt BETWEEN :V_START_CLM_RECEPT_DT
                                                                            AND :V_END_CLM_RECEPT_DT
        AND d.clh_trk_id IS NOT NULL and clm.vendor_cd = ''CH'')
 ,

providers AS
(SELECT DISTINCT
billProvider.provider_tax_id AS tax_id_number,
billProvider.provider_name,
billProvider.provider_name_first,
billProvider.provider_name_middle,
billProvider.provider_address_1,
billProvider.provider_address_2,
billProvider.provider_city,
billProvider.provider_state AS provider_state_code,
billProvider.provider_postalcode AS provider_zip,
billProvider.provider_id AS bill_prv_id,
A.ref_prv_id,
A.ref_prv_name_suffix,
A.ref_prv_name_last,
A.ref_prv_name_first,
A.ref_prv_name_middle,
A.transactset_create_date,
A.trancactset_cntl_no,
A.grp_control_no,
A.payer_hl_no,
A.provider_hl_no,
A.subscriber_hl_no,
A.xml_md5
FROM (Select GET(element.value,''prv_id''):: varchar AS ref_prv_id,
GET(element.value,''prv_name_suffix''):: varchar AS ref_prv_name_suffix,
GET(element.value,''prv_name_last''):: varchar AS ref_prv_name_last,
GET(element.value,''prv_name_first''):: varchar AS ref_prv_name_first,
GET(element.value,''prv_name_middle''):: varchar AS ref_prv_name_middle,
transactset_create_date,
trancactset_cntl_no,
grp_control_no,
payer_hl_no,
provider_hl_no,
subscriber_hl_no,xml_md5 from IDENTIFIER(:V_PROF_PROVIDER_ALL) prof_p,
LATERAL FLATTEN(to_array(prof_p.clm_referring_prv_map)) element) A
JOIN IDENTIFIER(:V_PROF_PROVIDER) billProvider
        ON (A.grp_control_no=billProvider.grp_control_no
        AND A.trancactset_cntl_no=billProvider.trancactset_cntl_no
        AND A.transactset_create_date=billProvider.transactset_create_date
        AND A.provider_hl_no = billProvider.provider_hl_no
        AND A.xml_md5 = billProvider.xml_md5)
WHERE length(A.ref_prv_id) > 0
AND A.transactset_create_date BETWEEN :V_START_DATE_TRANSACTSET
    AND :V_END_DATE_TRANSACTSET
    
    )


    
SELECT DISTINCT
receipt_dt AS "receipt_dt",
claim_no AS "claim_no",
bill_line_no AS "bill_line_no",
tax_id_number AS "tax_id_number",
provider_name AS "provider_name",
provider_name_first AS "provider_name_first",
provider_name_middle AS "provider_name_middle",
provider_address_1 AS "provider_address_1",
provider_address_2 AS "provider_address_2",
provider_city AS "provider_city",
provider_state_code AS "provider_state_code",
provider_zip AS "provider_zip",
split_part(service_date,''-'',1) AS "service_date",
CPT_codes AS "CPT_codes",
CAST(total_claim_charge_amt AS DECIMAL(12,2)) AS "total_claim_charge_amt",
CAST(line_item_charge_amt AS DECIMAL(12,2)) AS "line_item_charge_amt",
ref_prv_id AS "ref_prv_id",
ref_prv_name_last AS "ref_prv_name_last",
ref_prv_name_first AS "ref_prv_name_first",
ref_prv_name_middle AS "ref_prv_name_middle"
FROM claims clm
JOIN providers prvA
ON clm.transactset_create_date = prvA.transactset_create_date
        AND clm.trancactset_cntl_no = prvA.trancactset_cntl_no
        AND clm.grp_control_no = prvA.grp_control_no
        AND clm.payer_hl_no = prvA.payer_hl_no
        AND clm.provider_hl_no = prvA.provider_hl_no
        AND clm.subscriber_hl_no = prvA.subscriber_hl_no
        AND clm.xml_md5 = prvA.xml_md5
WHERE (CPT_codes BETWEEN ''L0112'' AND ''L4631'' OR  CPT_codes IN (''A4362'',''A4367'',''A4371'',''A4385'',''A4390'',''A4394'',''A4396'',''A4406'',''A4407'',''A4425'',''A4450'',''A4452'',''A4455'',''A4456'',''A5120'',''A6021'',
  ''A6023'',''A6209'',''A6235'',''A6236'',''A6238'',''A6244'',''A6248'',''A6402'',''A6403'',''A6531'',''E0691'',''E2100'',''E2510''))

  ;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_REF_PROVIDER_REPORT)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
V_STEP := ''STEP2'';

 
V_STEP_NAME := ''Generate Report for Ref Provider ''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/ref_provider_report/''||''Referring_Provider_''||(select TO_VARCHAR(:V_CURRENT_DATE,''YYYY''))||''_''||(select TO_VARCHAR(:V_CURRENT_DATE,''MM''))||''.csv''||'' FROM (
             SELECT *																															
               FROM TMP_REF_PROVIDER_REPORT
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = '''',''''
			   empty_field_as_null = false
               NULL_IF = ()
               compression = None
               FIELD_OPTIONALLY_ENCLOSED_BY =''''"''''
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
