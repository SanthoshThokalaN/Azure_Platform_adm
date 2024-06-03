USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE SP_FWA_PROFESSIONAL_SERVICE("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '

DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME             VARCHAR        default ''FWA_PERFORMANT'';
V_SUB_PROCESS_NAME         VARCHAR        default ''FWA_PROFESSIONAL_SERVICE'';
V_STEP                     VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER; 
V_ROWS_LOADED              INTEGER; 
V_MESSAGE                  VARCHAR;
V_LAST_QUERY_ID            VARCHAR; 
V_STAGE_QUERY1              VARCHAR;
V_STAGE_QUERY2              VARCHAR;

V_PREV_MONTH_START         VARCHAR;
V_PREV_MONTH_END           VARCHAR;
V_CLAIM_RECEIPT_START_DATE VARCHAR;
V_CLAIM_RECEIPT_END_DATE   VARCHAR;




V_TMP_FWA_PROFESSIONAL_SERVICE               VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_FWA_PROFESSIONAL_SERVICE'';



V_TMP_DB2IMPORT_CLM_FILTERED_NEW            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_DB2IMPORT_CLM_FILTERED_NEW'';
V_TMP_CLAIM                              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_CLAIM'';
V_TMP_SERVICE_PART1                         VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_SERVICE_PART1'';
V_TMP_PRV_ALL_PART_1                        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PRV_ALL_PART_1'';



V_PROF_CLAIM_PART          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_claim_part'';
V_PROF_CLM_SV_DATES        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_clm_sv_dates'';
V_PROF_AMBLNC_INFO         VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_amblnc_info'';
V_PROF_SERVICE             VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_service'';
V_PROF_SUBSCRIBER          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_subscriber'';
V_PROF_PROVIDER_ALL        VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_provider_all'';
V_CH_VIEW                  VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.ch_view'';



BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_PREV_MONTH_START := (SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYYMMDD''));
V_PREV_MONTH_END :=   (SELECT TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE))), ''YYYYMMDD''));

V_CLAIM_RECEIPT_START_DATE :=  (SELECT TO_VARCHAR(DATEADD(MONTH, -3, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYYMMDD''));
V_CLAIM_RECEIPT_END_DATE :=  (SELECT TO_VARCHAR(LAST_DAY(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE))), ''YYYYMMDD''));



V_STEP := ''STEP1'';
   
V_STEP_NAME := ''Load TMP_DB2IMPORT_CLM_FILTERED_NEW'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());



CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_TMP_DB2IMPORT_CLM_FILTERED_NEW) AS
SELECT db2.* FROM IDENTIFIER(:V_CH_VIEW) db2 WHERE TO_NUMBER(TO_VARCHAR(db2.clm_recept_dt::date, ''YYYYMMDD'')) BETWEEN TO_NUMBER(:V_CLAIM_RECEIPT_START_DATE) AND TO_NUMBER(:V_CLAIM_RECEIPT_END_DATE);

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_NEW)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   


                                 
V_STEP := ''STEP2'';
   
V_STEP_NAME := ''Load TMP_CLAIM'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_TMP_CLAIM) AS 
SELECT 
clmbase.transactset_create_date,
clmbase.grp_control_no,
clmbase.trancactset_cntl_no,
clmbase.provider_hl_no,
clmbase.subscriber_hl_no,
clmbase.payer_hl_no,
clmbase.claim_id,
clmbase.product_service_id_qlfr,
clmbase.line_item_charge_amt as line_item_charge_amt,
clmbase.service_unit_count as service_unit_count,
clmbase.payer_clm_ctrl_num,
clmbase.sl_seq_num,
clmbase.network_trace_number,
clmbase.service_date,
SPLIT(clmbase.product_service_id_qlfr,'':'')[1]::string as procedure_cd,
MONTH(TO_DATE(clmbase.transactset_create_date, ''yyyymmdd'')) as dl_clm_month,
YEAR(TO_DATE(clmbase.transactset_create_date, ''yyyymmdd'')) as dl_clm_year,
clmbase.diagnosis_code_pointers,
clmbase.emergency_indicator,
clmbase.family_planning_indicator,
clmbase.cas_adj_group_code,
clmbase.cas_adj_reason_code,
clmbase.cas_adj_amt,
clmbase.vendor_cd,
clmbase.xml_md5
FROM 


IDENTIFIER (:V_PROF_CLAIM_PART) clmbase
WHERE TO_NUMBER(clmbase.transactset_create_date) between TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END);


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_CLAIM)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   




V_STEP := ''STEP3'';
   
V_STEP_NAME := ''Load TMP_SERVICE_PART1'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_TMP_SERVICE_PART1) AS
SELECT DISTINCT t1.* FROM (
SELECT
db2.clm_num as ucps_clm_num,
db2.clm_recept_dt as ucps_clm_dt,
line.claim_id as patient_control_number,
line.sl_seq_num as bill_line_num,
line.dmec_mni_cert_transmission_cd as dmec_mni_cert_transmission_cd ,
line.dmec_type_cd as dmec_type_cd,
line.dmec_duration_measure_unit as dmec_duration_measure_unit,
line.dmec_duration as dmec_duration,
line.dmec_mni_condition_indicator as dmec_mni_condition_indicator,
line.dmec_mni_condition_cd_1 as dmec_mni_condition_cd_1,
line.dmec_mni_condition_cd_2 as dmec_mni_condition_cd_2,


test_measure_results.test_measure_ref_id as test_measure_ref_id,
test_measure_results.test_measure_type as test_measure_type,
test_measure_results.test_measure_results as test_measure_results,



line.clincal_lab_imprvment_amndment_num as clincal_lab_imprvment_amndment_num,
line.referring_clia_number as referring_clia_number,
SPLIT(line.medical_procedure_id,'':'')[0]::string as medical_procedure_id,
SPLIT(line.medical_procedure_id,'':'')[1]::string as medical_procedure_cd,
line.medical_necessity_measure_unit as medical_necessity_measure_unit,
line.medical_necessity_length as medical_necessity_length,
line.dme_rental_price as dme_rental_price,
line.dme_purchase_price as dme_purchase_price,
line.dme_frequency_cd as dme_frequency_cd,
IFF(REGEXP_INSTR(clm.service_date,''-'') > 0, SPLIT(clm.service_date,''-'')[0], clm.service_date) as first_date_of_service,
IFF(REGEXP_INSTR(clm.service_date,''-'') > 0, SPLIT(clm.service_date,''-'')[1], clm.service_date) as last_date_of_service,
line.sv_facility_type_qlfr as service_facility_type_qlfr,
line.sv_facility_name as service_facility_name,
line.sv_facility_primary_id as service_facility_primary_id,
line.sv_facility_ref_id_qlfr_1 as service_facility_ref_id_qlfr_1,
line.sv_facility_secondary_id_1 as service_facility_secondary_id_1,
line.sv_facility_secondary_id_2 as service_facility_secondary_id_2,
line.sv_facility_addr_1 as service_facility_addr_1,
line.sv_facility_addr_2 as service_facility_addr_2,
line.sv_facility_city as service_facility_city,
line.sv_facility_state as service_facility_state,
line.sv_facility_zip as service_facility_zip,
dates.certificate_revision_date as service_certificate_revision_date,
dates.begin_therapy_date as service_begin_therapy_date,
dates.treatment_therapy_date as service_treatment_therapy_date,
dates.last_certification_date as last_certification_date,
dates.shipped_date as service_shipped_date,
dates.initial_treatment_date as initial_treatment_date,
dates.prescription_date as service_prescription_date,
amblnc.sv_amblnc_weight_measure_unit as service_amblnc_weight_measure_unit,
amblnc.sv_amblnc_pat_weight as service_amblnc_pat_weight,
amblnc.sv_amblnc_transport_reason_cd as service_amblnc_transport_reason_cd,
amblnc.sv_amblnc_distance_measure_unit as service_amblnc_distance_measure_unit,
amblnc.sv_amblnc_transport_distance as service_amblnc_transport_distance,
amblnc.sv_amblnc_roundtrip_desc as service_amblnc_roundtrip_desc,
amblnc.sv_amblnc_stretcher_desc as service_amblnc_stretcher_desc,

sv_amblnc_cert.sv_amblnc_cert_condition_indicator as service_amblnc_cert_condition_indicator,
sv_amblnc_cert.sv_amblnc_condition_codes as service_amblnc_condition_codes,


//CONCAT_WS(''*'',sv_amblnc_cert.sv_amblnc_cert_condition_indicator) as service_amblnc_cert_condition_indicator,
//CONCAT_WS(''*'',sv_amblnc_cert.sv_amblnc_condition_codes[0],sv_amblnc_cert.sv_amblnc_condition_codes[1],sv_amblnc_cert.sv_amblnc_condition_codes[2]) as service_amblnc_condition_codes,


amblnc.sv_amblnc_pat_count as service_amblnc_pat_count,
amblnc.sv_amblnc_pickup_addr_1 as service_amblnc_pickup_addr_1,
amblnc.sv_amblnc_pickup_addr_2 as service_amblnc_pickup_addr_2,
amblnc.sv_amblnc_dropoff_addr_1 as service_amblnc_dropoff_addr_1,
amblnc.sv_amblnc_dropoff_addr_2 as service_amblnc_dropoff_addr_2,
amblnc.sv_amblnc_dropoff_location as service_amblnc_dropoff_location,
CONCAT(dates.grp_control_no,''$'',dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id,''$'',dates.sl_seq_num) as serviceuniquekey,
SPLIT(clm.product_service_id_qlfr,'':'')[0]::string as product_service_id_qlfr,
SPLIT(clm.product_service_id_qlfr,'':'')[1]::string as procedure_cd,
CONCAT_WS(''*'',SPLIT(clm.product_service_id_qlfr,'':'')[2],SPLIT(clm.product_service_id_qlfr,'':'')[3],SPLIT(clm.product_service_id_qlfr,'':'')[4],SPLIT(clm.product_service_id_qlfr,'':'')[5])::string as procedure_modifiers,
clm.line_item_charge_amt as line_item_charge_amt,
clm.service_unit_count as service_unit_count,
clm.transactset_create_date,
clm.diagnosis_code_pointers,
clm.emergency_indicator,
clm.family_planning_indicator,
line.drug_product_id_qlfr,
line.drug_product_id,
line.drug_unit_count,
line.drug_measure_unit,
clm.cas_adj_group_code,
clm.cas_adj_reason_code,
clm.cas_adj_amt,
clm.xml_md5
FROM IDENTIFIER (:V_TMP_CLAIM) clm LEFT OUTER JOIN IDENTIFIER (:V_PROF_SUBSCRIBER) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date
                                                 AND mem.xml_md5 = clm.xml_md5)
JOIN IDENTIFIER (:V_TMP_DB2IMPORT_CLM_FILTERED_NEW) db2 ON (db2.member_id = mem.subscriber_id
AND db2.medicare_clm_cntrl_num IS NOT NULL
AND db2.medicare_clm_cntrl_num = clm.payer_clm_ctrl_num
AND clm.vendor_cd =''CMS'')
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.provider_hl_no,
b.subscriber_hl_no,
b.payer_hl_no	,
b.claim_id	,
b.sl_seq_num,
b.transactset_create_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.certificate_revision_date)),'''')::string,3,8) as certificate_revision_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.begin_therapy_date)),'''')::string,3,8) as begin_therapy_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.treatment_therapy_date)),'''')::string,3,8) as treatment_therapy_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.initial_treatment_date)),'''')::string,3,8) as initial_treatment_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.shipped_date)),'''')::string,3,8) as shipped_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.prescription_date)),'''')::string,3,8) as prescription_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.last_certification_date)),'''')::string,3,8) as last_certification_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(concat_ws('''',b.service_date)),'''')::string,3,8) as service_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.sl_seq_num,
a.transactset_create_date,

        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''607''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as certificate_revision_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''463''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as begin_therapy_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''304''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as treatment_therapy_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''454''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as initial_treatment_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''011''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as shipped_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''471''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as prescription_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''461''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as last_certification_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''472''], '','')::VARCHAR, ''\\\\[|\\\\]''), '']'' ), ''[]'') as service_date
        
        
//ARRAY_AGG(a.group_map[''607'']) as certificate_revision_date,
//ARRAY_AGG(a.group_map[''463'']) as begin_therapy_date,
//ARRAY_AGG(a.group_map[''304'']) as treatment_therapy_date,
//ARRAY_AGG(a.group_map[''454'']) as initial_treatment_date,
//ARRAY_AGG(a.group_map[''011'']) as shipped_date,
//ARRAY_AGG(a.group_map[''471'']) as prescription_date,
//ARRAY_AGG(a.group_map[''461'']) as last_certification_date,
//ARRAY_AGG(a.group_map[''472'']) as service_date		


FROM (SELECT DISTINCT grp_control_no,
trancactset_cntl_no,
provider_hl_no,
subscriber_hl_no,
payer_hl_no,
claim_id,
sl_seq_num,
transactset_create_date,

OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map





FROM IDENTIFIER (:V_PROF_CLM_SV_DATES) WHERE sl_seq_num IS NOT null AND TO_NUMBER(transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)) a
GROUP BY a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
sl_seq_num,
a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id,''$'',dates.sl_seq_num) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
AND dates.transactset_create_date = clm.transactset_create_date)

LEFT OUTER JOIN IDENTIFIER(:V_PROF_AMBLNC_INFO) amblnc ON (amblnc.grp_control_no = clm.grp_control_no
AND concat(amblnc.trancactset_cntl_no,''$'',amblnc.provider_hl_no,''$'',amblnc.subscriber_hl_no,''$'',amblnc.claim_id,''$'',amblnc.sv_lx_number) = 
concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
AND amblnc.transactset_create_date = clm.transactset_create_date)

LEFT OUTER JOIN 
(
select grp_control_no,trancactset_cntl_no,provider_hl_no,subscriber_hl_no,claim_id,sv_lx_number, transactset_create_date , 
array_agg(value:sv_amblnc_cert_condition_indicator) as sv_amblnc_cert_condition_indicator, array_agg(value:sv_amblnc_condition_codes) as sv_amblnc_condition_codes from IDENTIFIER(:V_PROF_AMBLNC_INFO)
, table(flatten(parse_json(SV_AMBLNC_CERTIFICATION))) 
WHERE TO_NUMBER(transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)
GROUP BY grp_control_no,trancactset_cntl_no,provider_hl_no,subscriber_hl_no,claim_id,sv_lx_number, transactset_create_date 

)  sv_amblnc_cert 
  
ON (clm.grp_control_no = sv_amblnc_cert.grp_control_no
AND concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num) = 
  concat(sv_amblnc_cert.trancactset_cntl_no,''$'',sv_amblnc_cert.provider_hl_no,''$'',sv_amblnc_cert.subscriber_hl_no,''$'',sv_amblnc_cert.claim_id,''$'',sv_amblnc_cert.sv_lx_number)
AND clm.transactset_create_date = sv_amblnc_cert.transactset_create_date)
  
  

JOIN IDENTIFIER (:V_PROF_SERVICE) line ON (line.grp_control_no = clm.grp_control_no
AND concat(line.trancactset_cntl_no,''$'',line.provider_hl_no,''$'',line.subscriber_hl_no,''$'',line.claim_id,''$'',line.sl_seq_num) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
AND line.transactset_create_date = clm.transactset_create_date
AND line.xml_md5 = clm.xml_md5) 
  
LEFT OUTER JOIN 
(
select grp_control_no,trancactset_cntl_no,provider_hl_no,subscriber_hl_no,claim_id,sl_seq_num, transactset_create_date , 
array_agg(value:test_measure_ref_id) as test_measure_ref_id, array_agg(value:test_measure_type) as test_measure_type, array_agg(value:test_measure_results) as test_measure_results, xml_md5 from 
IDENTIFIER(:V_PROF_SERVICE), table(flatten(parse_json(TEST_MEASURE_RESULTS))) 
WHERE TO_NUMBER(transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)
GROUP BY grp_control_no,trancactset_cntl_no,provider_hl_no,subscriber_hl_no,claim_id,sl_seq_num, transactset_create_date, xml_md5 

)  test_measure_results 
  
 
  
ON (clm.grp_control_no = test_measure_results.grp_control_no
AND concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num) = concat(test_measure_results.trancactset_cntl_no,''$'',test_measure_results.provider_hl_no,''$'',
  test_measure_results.subscriber_hl_no,''$'',test_measure_results.claim_id,''$'',test_measure_results.sl_seq_num)
AND clm.transactset_create_date = test_measure_results.transactset_create_date
AND clm.xml_md5 = test_measure_results.xml_md5)  
  


WHERE TO_NUMBER(line.transactset_create_date)  BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END) 
AND TO_NUMBER(amblnc.transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)
  
  
UNION ALL
SELECT
db2.clm_num as ucps_clm_num,
db2.clm_recept_dt as ucps_clm_dt,
line.claim_id as patient_control_number,
line.sl_seq_num as bill_line_num,
line.dmec_mni_cert_transmission_cd as dmec_mni_cert_transmission_cd ,
line.dmec_type_cd as dmec_type_cd,
line.dmec_duration_measure_unit as dmec_duration_measure_unit,
line.dmec_duration as dmec_duration,
line.dmec_mni_condition_indicator as dmec_mni_condition_indicator,
line.dmec_mni_condition_cd_1 as dmec_mni_condition_cd_1,
line.dmec_mni_condition_cd_2 as dmec_mni_condition_cd_2,


test_measure_results.test_measure_ref_id as test_measure_ref_id,
test_measure_results.test_measure_type as test_measure_type,
test_measure_results.test_measure_results as test_measure_results,



line.clincal_lab_imprvment_amndment_num as clincal_lab_imprvment_amndment_num,
line.referring_clia_number as referring_clia_number,
SPLIT(line.medical_procedure_id,'':'')[0]::string as medical_procedure_id,
SPLIT(line.medical_procedure_id,'':'')[1]::string as medical_procedure_cd,
line.medical_necessity_measure_unit as medical_necessity_measure_unit,
line.medical_necessity_length as medical_necessity_length,
line.dme_rental_price as dme_rental_price,
line.dme_purchase_price as dme_purchase_price,
line.dme_frequency_cd as dme_frequency_cd,
IFF(REGEXP_INSTR(clm.service_date,''-'') > 0, SPLIT(clm.service_date,''-'')[0], clm.service_date) as first_date_of_service,
IFF(REGEXP_INSTR(clm.service_date,''-'') > 0, SPLIT(clm.service_date,''-'')[1], clm.service_date) as last_date_of_service,
line.sv_facility_type_qlfr as service_facility_type_qlfr,
line.sv_facility_name as service_facility_name,
line.sv_facility_primary_id as service_facility_primary_id,
line.sv_facility_ref_id_qlfr_1 as service_facility_ref_id_qlfr_1,
line.sv_facility_secondary_id_1 as service_facility_secondary_id_1,
line.sv_facility_secondary_id_2 as service_facility_secondary_id_2,
line.sv_facility_addr_1 as service_facility_addr_1,
line.sv_facility_addr_2 as service_facility_addr_2,
line.sv_facility_city as service_facility_city,
line.sv_facility_state as service_facility_state,
line.sv_facility_zip as service_facility_zip,
dates.certificate_revision_date as service_certificate_revision_date,
dates.begin_therapy_date as service_begin_therapy_date,
dates.treatment_therapy_date as service_treatment_therapy_date,
dates.last_certification_date as last_certification_date,
dates.shipped_date as service_shipped_date,
dates.initial_treatment_date as initial_treatment_date,
dates.prescription_date as service_prescription_date,
amblnc.sv_amblnc_weight_measure_unit as service_amblnc_weight_measure_unit,
amblnc.sv_amblnc_pat_weight as service_amblnc_pat_weight,
amblnc.sv_amblnc_transport_reason_cd as service_amblnc_transport_reason_cd,
amblnc.sv_amblnc_distance_measure_unit as service_amblnc_distance_measure_unit,
amblnc.sv_amblnc_transport_distance as service_amblnc_transport_distance,
amblnc.sv_amblnc_roundtrip_desc as service_amblnc_roundtrip_desc,
amblnc.sv_amblnc_stretcher_desc as service_amblnc_stretcher_desc,
  
sv_amblnc_cert.sv_amblnc_cert_condition_indicator as service_amblnc_cert_condition_indicator,
sv_amblnc_cert.sv_amblnc_condition_codes as service_amblnc_condition_codes,
  
//CONCAT_WS(''*'',sv_amblnc_cert.sv_amblnc_cert_condition_indicator) as service_amblnc_cert_condition_indicator,
//CONCAT_WS(''*'',sv_amblnc_cert.sv_amblnc_condition_codes[0],sv_amblnc_cert.sv_amblnc_condition_codes[1],sv_amblnc_cert.sv_amblnc_condition_codes[2]) as service_amblnc_condition_codes,
  
  
amblnc.sv_amblnc_pat_count as service_amblnc_pat_count,
amblnc.sv_amblnc_pickup_addr_1 as service_amblnc_pickup_addr_1,
amblnc.sv_amblnc_pickup_addr_2 as service_amblnc_pickup_addr_2,
amblnc.sv_amblnc_dropoff_addr_1 as service_amblnc_dropoff_addr_1,
amblnc.sv_amblnc_dropoff_addr_2 as service_amblnc_dropoff_addr_2,
amblnc.sv_amblnc_dropoff_location as service_amblnc_dropoff_location,
CONCAT(dates.grp_control_no,''$'',dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id,''$'',dates.sl_seq_num) as serviceuniquekey,
SPLIT(clm.product_service_id_qlfr,'':'')[0]::string as product_service_id_qlfr,
SPLIT(clm.product_service_id_qlfr,'':'')[1]::string as procedure_cd,
CONCAT_WS(''*'',SPLIT(clm.product_service_id_qlfr,'':'')[2],SPLIT(clm.product_service_id_qlfr,'':'')[3],SPLIT(clm.product_service_id_qlfr,'':'')[4],SPLIT(clm.product_service_id_qlfr,'':'')[5])::string as procedure_modifiers,
clm.line_item_charge_amt as line_item_charge_amt,
clm.service_unit_count as service_unit_count,
clm.transactset_create_date,
clm.diagnosis_code_pointers,
clm.emergency_indicator,
clm.family_planning_indicator,
line.drug_product_id_qlfr,
line.drug_product_id,
line.drug_unit_count,
line.drug_measure_unit,
clm.cas_adj_group_code,
clm.cas_adj_reason_code,
clm.cas_adj_amt,
clm.xml_md5
FROM IDENTIFIER (:V_TMP_CLAIM) CLM LEFT OUTER JOIN IDENTIFIER (:V_PROF_SUBSCRIBER) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date
                                                 AND mem.xml_md5 = clm.xml_md5)
JOIN IDENTIFIER (:V_TMP_DB2IMPORT_CLM_FILTERED_NEW)  db2 ON (db2.clh_trk_id = clm.network_trace_number
AND db2.clh_trk_id IS NOT NULL
AND clm.vendor_cd=''CH'')
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.provider_hl_no,
b.subscriber_hl_no,
b.payer_hl_no	,
b.claim_id	,
b.sl_seq_num,
b.transactset_create_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.certificate_revision_date)),'''')::string,3,8) as certificate_revision_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.begin_therapy_date)),'''')::string,3,8) as begin_therapy_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.treatment_therapy_date)),'''')::string,3,8) as treatment_therapy_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.initial_treatment_date)),'''')::string,3,8) as initial_treatment_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.shipped_date)),'''')::string,3,8) as shipped_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.prescription_date)),'''')::string,3,8) as prescription_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.last_certification_date)),'''')::string,3,8) as last_certification_date,
SUBSTR(ARRAY_TO_STRING(PARSE_JSON(CONCAT_WS('''',b.service_date)),'''')::string,3,8) as service_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.sl_seq_num,
a.transactset_create_date,

        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''607''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as certificate_revision_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''463''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as begin_therapy_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''304''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as treatment_therapy_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''454''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as initial_treatment_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''011''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as shipped_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''471''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as prescription_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''461''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as last_certification_date,
        COALESCE(CONCAT(''['', REGEXP_REPLACE(LISTAGG(a.group_map[''472''], '','')::VARCHAR, ''\\\\\\\\[|\\\\\\\\]''), '']'' ), ''[]'') as service_date


//ARRAY_AGG(a.group_map[''607'']) as certificate_revision_date,
//ARRAY_AGG(a.group_map[''463'']) as begin_therapy_date,
//ARRAY_AGG(a.group_map[''304'']) as treatment_therapy_date,
//ARRAY_AGG(a.group_map[''454'']) as initial_treatment_date,
//ARRAY_AGG(a.group_map[''011'']) as shipped_date,
//ARRAY_AGG(a.group_map[''471'']) as prescription_date,
//ARRAY_AGG(a.group_map[''461'']) as last_certification_date,
//ARRAY_AGG(a.group_map[''472'']) as service_date



FROM (SELECT distinct grp_control_no,
trancactset_cntl_no,
provider_hl_no,
subscriber_hl_no,
payer_hl_no,
claim_id,
sl_seq_num,
transactset_create_date,
  
OBJECT_CONSTRUCT(clm_date_type,ARRAY_CONSTRUCT(clm_date)) as group_map

  
  
FROM IDENTIFIER (:V_PROF_CLM_SV_DATES) where sl_seq_num IS NOT NULL AND TO_NUMBER(transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)) a
GROUP BY a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
sl_seq_num,
a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id,''$'',dates.sl_seq_num) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
AND dates.transactset_create_date = clm.transactset_create_date)

LEFT OUTER JOIN IDENTIFIER(:V_PROF_AMBLNC_INFO) amblnc ON (amblnc.grp_control_no = clm.grp_control_no
AND concat(amblnc.trancactset_cntl_no,''$'',amblnc.provider_hl_no,''$'',amblnc.subscriber_hl_no,''$'',amblnc.claim_id,''$'',amblnc.sv_lx_number) = 
  concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
AND amblnc.transactset_create_date = clm.transactset_create_date)
  
LEFT OUTER JOIN 
(
select grp_control_no,trancactset_cntl_no,provider_hl_no,subscriber_hl_no,claim_id,sv_lx_number, transactset_create_date , 
array_agg(value:sv_amblnc_cert_condition_indicator) as sv_amblnc_cert_condition_indicator, array_agg(value:sv_amblnc_condition_codes) as sv_amblnc_condition_codes from IDENTIFIER(:V_PROF_AMBLNC_INFO)
, table(flatten(parse_json(SV_AMBLNC_CERTIFICATION))) 
WHERE TO_NUMBER(transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)
GROUP BY grp_control_no,trancactset_cntl_no,provider_hl_no,subscriber_hl_no,claim_id,sv_lx_number, transactset_create_date 

)  sv_amblnc_cert 
  
ON (clm.grp_control_no = sv_amblnc_cert.grp_control_no
AND concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num) = 
  concat(sv_amblnc_cert.trancactset_cntl_no,''$'',sv_amblnc_cert.provider_hl_no,''$'',sv_amblnc_cert.subscriber_hl_no,''$'',sv_amblnc_cert.claim_id,''$'',sv_amblnc_cert.sv_lx_number)
AND clm.transactset_create_date = sv_amblnc_cert.transactset_create_date)
  

JOIN IDENTIFIER(:V_PROF_SERVICE) line ON (line.grp_control_no = clm.grp_control_no
AND concat(line.trancactset_cntl_no,''$'',line.provider_hl_no,''$'',line.subscriber_hl_no,''$'',line.claim_id,''$'',line.sl_seq_num) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num)
AND line.transactset_create_date = clm.transactset_create_date
AND line.xml_md5 = clm.xml_md5) 
  
LEFT OUTER JOIN 
(
select grp_control_no,trancactset_cntl_no,provider_hl_no,subscriber_hl_no,claim_id,sl_seq_num, transactset_create_date , 
array_agg(value:test_measure_ref_id) as test_measure_ref_id, array_agg(value:test_measure_type) as test_measure_type, array_agg(value:test_measure_results) as test_measure_results, xml_md5 from 
IDENTIFIER(:V_PROF_SERVICE), table(flatten(parse_json(TEST_MEASURE_RESULTS))) 
WHERE TO_NUMBER(transactset_create_date) BETWEEN TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)
GROUP BY grp_control_no,trancactset_cntl_no,provider_hl_no,subscriber_hl_no,claim_id,sl_seq_num, transactset_create_date, xml_md5

)  test_measure_results 
  
 
  
ON (clm.grp_control_no = test_measure_results.grp_control_no
AND concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id,''$'',clm.sl_seq_num) = concat(test_measure_results.trancactset_cntl_no,''$'',test_measure_results.provider_hl_no,''$'',
  test_measure_results.subscriber_hl_no,''$'',test_measure_results.claim_id,''$'',test_measure_results.sl_seq_num)
AND clm.transactset_create_date = test_measure_results.transactset_create_date
AND clm.xml_md5 = test_measure_results.xml_md5)  
  
  
WHERE TO_NUMBER(line.transactset_create_date) between TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)
and TO_NUMBER(amblnc.transactset_create_date) between TO_NUMBER(:V_PREV_MONTH_START) AND TO_NUMBER(:V_PREV_MONTH_END)


) t1;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_SERVICE_PART1)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);      




V_STEP := ''STEP4'';
   
V_STEP_NAME := ''Load TMP_PRV_ALL_PART_1'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER (:V_TMP_PRV_ALL_PART_1) AS
SELECT DISTINCT 
prvall.sv_rendering_prv_map[''prv_type_qlfr'']::string as rendering_prv_type_qlfr,
prvall.sv_rendering_prv_map[''prv_name_last'']::string as rendering_prv_name_last,
prvall.sv_rendering_prv_map[''prv_name_first'']::string as rendering_prv_name_first,
prvall.sv_rendering_prv_map[''prv_name_middle'']::string as rendering_prv_name_middle,
prvall.sv_rendering_prv_map[''prv_name_suffix'']::string as rendering_prv_name_suffix,
prvall.sv_rendering_prv_map[''prv_id'']::string as rendering_prv_id,
prvall.sv_rendering_prv_map[''prv_speciality_id_qlfr'']::string as rendering_speciality_id_qlfr,
prvall.sv_rendering_prv_map[''prv_speciality_tax_code'']::string as rendering_prv_speciality_tax_code,
prvall.sv_rendering_prv_map[''prv_ref_id_qlfy'']::string as rendering_prv_ref_id_qlfy,
prvall.sv_rendering_prv_map[''prv_second_id'']::string as rendering_prv_second_id,
prvall.sv_rendering_prv_map[''other_payer_ren_prv_type'']::string as rendering_other_payer_ren_prv_type,
prvall.sv_supervising_prv_map[''prv_type_qlfr'']::string as supervising_prv_type_qlfr,
prvall.sv_supervising_prv_map[''prv_name_last'']::string as supervising_prv_name_last,
prvall.sv_supervising_prv_map[''prv_name_first'']::string as supervising_prv_name_first,
prvall.sv_supervising_prv_map[''prv_name_middle'']::string as supervising_prv_name_middle,
prvall.sv_supervising_prv_map[''prv_name_suffix'']::string as supervising_prv_name_suffix,
prvall.sv_supervising_prv_map[''prv_id'']::string as supervising_prv_id,
prvall.sv_supervising_prv_map[''prv_ref_id_qlfy'']::string as supervising_prv_ref_id_qlfy,
prvall.sv_supervising_prv_map[''prv_second_id'']::string as supervising_prv_second_id,
prvall.sv_supervising_prv_map[''prv_otherpayer_refer_id_qlfy'']::string as supervising_prv_otherpayer_refer_id_qlfy,
prvall.sv_supervising_prv_map[''prv_otherpayer_id'']::string as supervising_prv_otherpayer_id,
prvall.sv_referring_prv_map[''prv_type_qlfr'']::string as referring_type_qlfr,
prvall.sv_referring_prv_map[''prv_name_last'']::string as referring_prv_name_last,
prvall.sv_referring_prv_map[''prv_name_first'']::string as referring_prv_name_first,
prvall.sv_referring_prv_map[''prv_name_middle'']::string as referring_prv_name_middle,
prvall.sv_referring_prv_map[''prv_name_suffix'']::string as referring_prv_name_suffix,
prvall.sv_referring_prv_map[''prv_id'']::string as referring_prv_id,
prvall.sv_referring_prv_map[''prv_ref_id_qlfy'']::string as referring_prv_ref_id_qlfy,
prvall.sv_referring_prv_map[''prv_second_id'']::string as referring_prv_second_id,
prvall.sv_ordering_prv_map[''prv_type_qlfr'']::string as ordering_type_qlfr,
prvall.sv_ordering_prv_map[''prv_name_last'']::string as ordering_prv_name_last,
prvall.sv_ordering_prv_map[''prv_name_first'']::string as ordering_prv_name_first,
prvall.sv_ordering_prv_map[''prv_name_middle'']::string as ordering_prv_name_middle,
prvall.sv_ordering_prv_map[''prv_name_suffix'']::string as ordering_prv_name_suffix,
prvall.sv_ordering_prv_map[''prv_id'']::string as ordering_prv_id,
prvall.sv_ordering_prv_map[''provider_city'']::string as ordering_provider_city,
prvall.sv_ordering_prv_map[''provider_zip'']::string as ordering_provider_zip,
prvall.sv_ordering_prv_map[''provider_state'']::string as ordering_provider_state,
prvall.sv_ordering_prv_map[''prv_ref_id_qlfy'']::string as ordering_prv_ref_id_qlfy,
prvall.sv_ordering_prv_map[''prv_second_id'']::string as ordering_prv_second_id,
prvall.sv_ordering_prv_map[''prv_otherpayer_refer_id_qlfy'']::string as ordering_prv_otherpayer_refer_id_qlfy,
prvall.sv_ordering_prv_map[''prv_otherpayer_id'']::string as ordering_prv_otherpayer_id,
prvall.sv_ordering_prv_map[''prv_contact_name'']::string as ordering_prv_contact_name,
prvall.sv_ordering_prv_map[''prv_contact_type_1'']::string as ordering_prv_contact_type_1,
prvall.sv_ordering_prv_map[''prv_contact_number_1'']::string as ordering_prv_contact_number_1,
prvall.sv_ordering_prv_map[''prv_contact_type_2'']::string as ordering_prv_contact_type_2,
prvall.sv_ordering_prv_map[''prv_contact_number_2'']::string as ordering_prv_contact_number_2,
prvall.sv_ordering_prv_map[''prv_contact_type_3'']::string as ordering_prv_contact_type_3,
prvall.sv_ordering_prv_map[''prv_contact_number_3'']::string as ordering_prv_contact_number_3,
prvall.trancactset_cntl_no,
prvall.transactset_create_date,
prvall.grp_control_no,
prvall.provider_hl_no,
prvall.subscriber_hl_no,
prvall.claim_id,
prvall.sl_seq_num,
prvall.xml_md5
FROM IDENTIFIER (:V_PROF_PROVIDER_ALL) prvall WHERE TO_NUMBER(prvall.transactset_create_date) between TO_NUMBER(:V_PREV_MONTH_START) AND    TO_NUMBER(:V_PREV_MONTH_END) ;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PRV_ALL_PART_1)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   




V_STEP := ''STEP5'';
   
V_STEP_NAME := ''Load TMP_FWA_PROFESSIONAL_SERVICE'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_FWA_PROFESSIONAL_SERVICE) AS
SELECT DISTINCT * FROM (SELECT
ucps_clm_num as "ucps_clm_num",
ucps_clm_dt as "ucps_clm_dt",
patient_control_number as "patient_control_number",
bill_line_num as "bill_line_num",
dmec_mni_cert_transmission_cd as "dmec_mni_cert_transmission_cd",
dmec_type_cd as "dmec_type_cd",
dmec_duration_measure_unit as "dmec_duration_measure_unit",
dmec_duration as "dmec_duration",
dmec_mni_condition_indicator as "dmec_mni_condition_indicator",
dmec_mni_condition_cd_1 as "dmec_mni_condition_cd_1",
dmec_mni_condition_cd_2 as "dmec_mni_condition_cd_2",
ARRAY_TO_STRING(test_measure_ref_id,'''') as "test_measure_ref_id",
ARRAY_TO_STRING(test_measure_type,'''') as "test_measure_type",
ARRAY_TO_STRING(test_measure_results,'''') as "test_measure_results",
clincal_lab_imprvment_amndment_num as "clincal_lab_imprvment_amndment_num",
referring_clia_number as "referring_clia_number",
medical_procedure_id as "medical_procedure_id",
medical_procedure_cd as "medical_procedure_cd",
medical_necessity_measure_unit as "medical_necessity_measure_unit",
medical_necessity_length as "medical_necessity_length",
dme_rental_price as "dme_rental_price",
dme_purchase_price as "dme_purchase_price",
dme_frequency_cd as "dme_frequency_cd",
service_facility_type_qlfr as "service_facility_type_qlfr",
service_facility_name as "service_facility_name",
service_facility_primary_id as "service_facility_primary_id",
service_facility_ref_id_qlfr_1 as "service_facility_ref_id_qlfr_1",
service_facility_secondary_id_1 as "service_facility_secondary_id_1",
service_facility_secondary_id_2 as "service_facility_secondary_id_2",
service_facility_addr_1 as "service_facility_addr_1",
service_facility_addr_2 as "service_facility_addr_2",
service_facility_city as "service_facility_city",
service_facility_state as "service_facility_state",
service_facility_zip as "service_facility_zip",
service_certificate_revision_date as "service_certificate_revision_date",
service_begin_therapy_date as "service_begin_therapy_date",
service_treatment_therapy_date as "service_treatment_therapy_date",
last_certification_date as "last_certification_date",
service_shipped_date as "service_shipped_date",
initial_treatment_date as "initial_treatment_date",
service_prescription_date as "service_prescription_date",
first_date_of_service as "first_date_of_service",
last_date_of_service as "last_date_of_service",
prvall.rendering_prv_type_qlfr as "rendering_prv_type_qlfr",
prvall.rendering_prv_name_last as "rendering_prv_name_last",
prvall.rendering_prv_name_first as "rendering_prv_name_first",
prvall.rendering_prv_name_middle as "rendering_prv_name_middle",
prvall.rendering_prv_name_suffix as "rendering_prv_name_suffix",
prvall.rendering_prv_id as "rendering_prv_id",
prvall.rendering_speciality_id_qlfr as "rendering_speciality_id_qlfr",
prvall.rendering_prv_speciality_tax_code as "rendering_prv_speciality_tax_code",
prvall.rendering_prv_ref_id_qlfy as "rendering_prv_ref_id_qlfy",
prvall.rendering_prv_second_id as "rendering_prv_second_id",
prvall.rendering_other_payer_ren_prv_type as "rendering_other_payer_ren_prv_type",
prvall.supervising_prv_type_qlfr as "supervising_prv_type_qlfr",
prvall.supervising_prv_name_last as "supervising_prv_name_last",
prvall.supervising_prv_name_first as "supervising_prv_name_first",
prvall.supervising_prv_name_middle as "supervising_prv_name_middle",
prvall.supervising_prv_name_suffix as "supervising_prv_name_suffix",
prvall.supervising_prv_id as "supervising_prv_id",
prvall.supervising_prv_ref_id_qlfy as "supervising_prv_ref_id_qlfy",
prvall.supervising_prv_second_id as "supervising_prv_second_id",
prvall.supervising_prv_otherpayer_refer_id_qlfy as "supervising_prv_otherpayer_refer_id_qlfy",
prvall.supervising_prv_otherpayer_id as "supervising_prv_otherpayer_id",
prvall.referring_type_qlfr as "referring_type_qlfr",
prvall.referring_prv_name_last as "referring_prv_name_last",
prvall.referring_prv_name_first as "referring_prv_name_first",
prvall.referring_prv_name_middle as "referring_prv_name_middle",
prvall.referring_prv_name_suffix as "referring_prv_name_suffix",
prvall.referring_prv_id as "referring_prv_id",
prvall.referring_prv_ref_id_qlfy as "referring_prv_ref_id_qlfy",
prvall.referring_prv_second_id as "referring_prv_second_id",
prvall.ordering_type_qlfr as "ordering_type_qlfr",
prvall.ordering_prv_name_last as "ordering_prv_name_last",
prvall.ordering_prv_name_first as "ordering_prv_name_first",
prvall.ordering_prv_name_middle as "ordering_prv_name_middle",
prvall.ordering_prv_name_suffix as "ordering_prv_name_suffix",
prvall.ordering_prv_id as "ordering_prv_id",
prvall.ordering_provider_city as "ordering_provider_city",
prvall.ordering_provider_zip as "ordering_provider_zip",
prvall.ordering_provider_state as "ordering_provider_state",
prvall.ordering_prv_ref_id_qlfy as "ordering_prv_ref_id_qlfy",
prvall.ordering_prv_second_id as "ordering_prv_second_id",
prvall.ordering_prv_otherpayer_refer_id_qlfy as "ordering_prv_otherpayer_refer_id_qlfy",
prvall.ordering_prv_otherpayer_id as "ordering_prv_otherpayer_id",
prvall.ordering_prv_contact_name as "ordering_prv_contact_name",
prvall.ordering_prv_contact_type_1 as "ordering_prv_contact_type_1",
prvall.ordering_prv_contact_number_1 as "ordering_prv_contact_number_1",
prvall.ordering_prv_contact_type_2 as "ordering_prv_contact_type_2",
prvall.ordering_prv_contact_number_2 as "ordering_prv_contact_number_2",
prvall.ordering_prv_contact_type_3 as "ordering_prv_contact_type_3",
prvall.ordering_prv_contact_number_3 as "ordering_prv_contact_number_3",
service_amblnc_weight_measure_unit as "service_amblnc_weight_measure_unit",
service_amblnc_pat_weight as "service_amblnc_pat_weight",
service_amblnc_transport_reason_cd as "service_amblnc_transport_reason_cd",
service_amblnc_distance_measure_unit as "service_amblnc_distance_measure_unit",
service_amblnc_transport_distance as "service_amblnc_transport_distance",
service_amblnc_roundtrip_desc as "service_amblnc_roundtrip_desc",
service_amblnc_stretcher_desc as "service_amblnc_stretcher_desc",
service_amblnc_cert_condition_indicator as "service_amblnc_cert_condition_indicator",
service_amblnc_condition_codes as "service_amblnc_condition_codes",
service_amblnc_pat_count as "service_amblnc_pat_count",
service_amblnc_pickup_addr_1 as "service_amblnc_pickup_addr_1",
service_amblnc_pickup_addr_2 as "service_amblnc_pickup_addr_2",
service_amblnc_dropoff_addr_1 as "service_amblnc_dropoff_addr_1",
service_amblnc_dropoff_addr_2 as "service_amblnc_dropoff_addr_2",
service_amblnc_dropoff_location as "service_amblnc_dropoff_location",
product_service_id_qlfr as "product_service_id_qlfr",
procedure_cd as "procedure_cd",
procedure_modifiers as "procedure_modifiers",
line_item_charge_amt as "line_item_charge_amt",
service_unit_count as "service_unit_count",
diagnosis_code_pointers as "diagnosis_code_pointers",
emergency_indicator as "emergency_indicator",
family_planning_indicator as "family_planning_indicator",
drug_product_id_qlfr as "drug_product_id_qlfr",
drug_product_id as "drug_product_id",
drug_unit_count as "drug_unit_count",
drug_measure_unit as "drug_measure_unit",
cas_adj_group_code as "cas_adj_group_code",
cas_adj_reason_code as "cas_adj_reason_code",
cas_adj_amt as "cas_adj_amt"

FROM IDENTIFIER (:V_TMP_SERVICE_PART1) s1
LEFT OUTER JOIN IDENTIFIER (:V_TMP_PRV_ALL_PART_1) prvall 
ON (	CONCAT(prvall.grp_control_no,''$'',prvall.trancactset_cntl_no,''$'',prvall.provider_hl_no,''$'',prvall.subscriber_hl_no,''$'',prvall.claim_id,''$'',prvall.sl_seq_num) = s1.serviceuniquekey 
AND prvall.transactset_create_date = s1.transactset_create_date
AND prvall.xml_md5 = s1.xml_md5)) t1;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_FWA_PROFESSIONAL_SERVICE)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
    
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   
  
V_STEP := ''STEP6'';
   
  
V_STEP_NAME := ''Generate FWA_PROF_SERVICE_REPORT''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      



V_STAGE_QUERY1 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_performant/''||''fwa_professional_service_report_''||(SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYY_MM''))||'' FROM (
             
     SELECT * FROM TMP_FWA_PROFESSIONAL_SERVICE
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''~''''
			   empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''')
               compression = ''''gzip'''' 

              )
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = False''
;
  
V_STAGE_QUERY2 := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/fwa_performant/''||''fwa_professional_service_report_''||(SELECT TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYY_MM''))||'' FROM (
             
SELECT 
''''ucps_clm_num'''',
''''ucps_clm_dt'''',
''''patient_control_number'''',
''''bill_line_num'''',
''''dmec_mni_cert_transmission_cd'''',
''''dmec_type_cd'''',
''''dmec_duration_measure_unit'''',
''''dmec_duration'''',
''''dmec_mni_condition_indicator'''',
''''dmec_mni_condition_cd_1'''',
''''dmec_mni_condition_cd_2'''',
''''test_measure_ref_id'''',
''''test_measure_type'''',
''''test_measure_results'''',
''''clincal_lab_imprvment_amndment_num'''',
''''referring_clia_number'''',
''''medical_procedure_id'''',
''''medical_procedure_cd'''',
''''medical_necessity_measure_unit'''',
''''medical_necessity_length'''',
''''dme_rental_price'''',
''''dme_purchase_price'''',
''''dme_frequency_cd'''',
''''service_facility_type_qlfr'''',
''''service_facility_name'''',
''''service_facility_primary_id'''',
''''service_facility_ref_id_qlfr_1'''',
''''service_facility_secondary_id_1'''',
''''service_facility_secondary_id_2'''',
''''service_facility_addr_1'''',
''''service_facility_addr_2'''',
''''service_facility_city'''',
''''service_facility_state'''',
''''service_facility_zip'''',
''''service_certificate_revision_date'''',
''''service_begin_therapy_date'''',
''''service_treatment_therapy_date'''',
''''last_certification_date'''',
''''service_shipped_date'''',
''''initial_treatment_date'''',
''''service_prescription_date'''',
''''first_date_of_service'''',
''''last_date_of_service'''',
''''rendering_prv_type_qlfr'''',
''''rendering_prv_name_last'''',
''''rendering_prv_name_first'''',
''''rendering_prv_name_middle'''',
''''rendering_prv_name_suffix'''',
''''rendering_prv_id'''',
''''rendering_speciality_id_qlfr'''',
''''rendering_prv_speciality_tax_code'''',
''''rendering_prv_ref_id_qlfy'''',
''''rendering_prv_second_id'''',
''''rendering_other_payer_ren_prv_type'''',
''''supervising_prv_type_qlfr'''',
''''supervising_prv_name_last'''',
''''supervising_prv_name_first'''',
''''supervising_prv_name_middle'''',
''''supervising_prv_name_suffix'''',
''''supervising_prv_id'''',
''''supervising_prv_ref_id_qlfy'''',
''''supervising_prv_second_id'''',
''''supervising_prv_otherpayer_refer_id_qlfy'''',
''''supervising_prv_otherpayer_id'''',
''''referring_type_qlfr'''',
''''referring_prv_name_last'''',
''''referring_prv_name_first'''',
''''referring_prv_name_middle'''',
''''referring_prv_name_suffix'''',
''''referring_prv_id'''',
''''referring_prv_ref_id_qlfy'''',
''''referring_prv_second_id'''',
''''ordering_type_qlfr'''',
''''ordering_prv_name_last'''',
''''ordering_prv_name_first'''',
''''ordering_prv_name_middle'''',
''''ordering_prv_name_suffix'''',
''''ordering_prv_id'''',
''''ordering_provider_city'''',
''''ordering_provider_zip'''',
''''ordering_provider_state'''',
''''ordering_prv_ref_id_qlfy'''',
''''ordering_prv_second_id'''',
''''ordering_prv_otherpayer_refer_id_qlfy'''',
''''ordering_prv_otherpayer_id'''',
''''ordering_prv_contact_name'''',
''''ordering_prv_contact_type_1'''',
''''ordering_prv_contact_number_1'''',
''''ordering_prv_contact_type_2'''',
''''ordering_prv_contact_number_2'''',
''''ordering_prv_contact_type_3'''',
''''ordering_prv_contact_number_3'''',
''''service_amblnc_weight_measure_unit'''',
''''service_amblnc_pat_weight'''',
''''service_amblnc_transport_reason_cd'''',
''''service_amblnc_distance_measure_unit'''',
''''service_amblnc_transport_distance'''',
''''service_amblnc_roundtrip_desc'''',
''''service_amblnc_stretcher_desc'''',
''''service_amblnc_cert_condition_indicator'''',
''''service_amblnc_condition_codes'''',
''''service_amblnc_pat_count'''',
''''service_amblnc_pickup_addr_1'''',
''''service_amblnc_pickup_addr_2'''',
''''service_amblnc_dropoff_addr_1'''',
''''service_amblnc_dropoff_addr_2'''',
''''service_amblnc_dropoff_location'''',
''''product_service_id_qlfr'''',
''''procedure_cd'''',
''''procedure_modifiers'''',
''''line_item_charge_amt'''',
''''service_unit_count'''',
''''diagnosis_code_pointers'''',
''''emergency_indicator'''',
''''family_planning_indicator'''',
''''drug_product_id_qlfr'''',
''''drug_product_id'''',
''''drug_unit_count'''',
''''drug_measure_unit'''',
''''cas_adj_group_code'''',
''''cas_adj_reason_code'''',
''''cas_adj_amt''''


               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''~''''
			   empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''')
               compression = ''''gzip''''  
              )
HEADER = False
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = False''
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
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);  
  

EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';