USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_MSPDP_EXTRACT"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "LZ_ISDW_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_CLAIM_RECEIPT_START_DATE VARCHAR :=   :V_CURRENT_DATE  - 60;

V_CLAIM_RECEIPT_END_DATE VARCHAR :=     :V_CURRENT_DATE;

V_CLAIM_PAID_START_DATE VARCHAR :=       TO_VARCHAR(:V_CURRENT_DATE-7, ''YYYYMMDD'');    

V_CLAIM_PAID_END_DATE VARCHAR :=    TO_VARCHAR(:V_CURRENT_DATE, ''YYYYMMDD'') ;

V_TRANSACTSET_CREATE_START_DATE VARCHAR := TO_VARCHAR(:V_CURRENT_DATE  - 90, ''YYYYMMDD'');

V_TRANSACTSET_CREATE_END_DATE VARCHAR := TO_VARCHAR(:V_CURRENT_DATE  + 3, ''YYYYMMDD'');


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''MSPDP_EXTRACT'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''MSPDP_EXTRACT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;



V_CH_VIEW                      varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.CH_VIEW'';

V_ISDW_SERVICE_DATES_IMPORT    varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.ISDW_SERVICE_DATES_IMPORT'';

V_INST_CLAIM_PART              varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLAIM_PART''; 

V_INST_SUBSCRIBER              varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_SUBSCRIBER'';

V_INPUT_ICD                    varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.INPUT_ICD'';

V_PROF_CLAIM_PART              varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLAIM_PART'';

V_PROF_SUBSCRIBER              varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_SUBSCRIBER'';

V_SUBSCRIBER_INPUT              varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.SUBSCRIBER_INPUT'';

V_TMP_DB2IMPORT_CLM_FILTERED_1     VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_DB2IMPORT_CLM_FILTERED_1'';

V_TMP_INST_CLAIM_TEMP_1            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_INST_CLAIM_TEMP_1'';

V_TMP_INST_MEMBER_TEMP             VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_INST_MEMBER_TEMP'';

V_TMP_INST_CLAIM_TEMP_NEW          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_INST_CLAIM_TEMP_NEW'';

V_TMP_PROF_CLAIM_TEMP_1            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PROF_CLAIM_TEMP_1'';

V_TMP_PROF_MEMBER_TEMP             VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PROF_MEMBER_TEMP'';

V_TMP_PROF_CLAIM_TEMP_NEW          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_PROF_CLAIM_TEMP_NEW'';


V_TMP_OUTPUT_COMBINED              varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_OUTPUT_COMBINED'';
V_TMP_FINAL_OUTPUT_COMBINED        varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.TMP_FINAL_OUTPUT_COMBINED'';



BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';
   
 
V_STEP_NAME := ''Load TMP_DB2IMPORT_CLM_FILTERED_1''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

   
CREATE  OR REPLACE TEMPORARY TABLE  IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_1) AS
SELECT db2.* FROM 
IDENTIFIER(:V_CH_VIEW)  db2 JOIN 
IDENTIFIER(:V_ISDW_SERVICE_DATES_IMPORT)  isdw on db2.clm_num = isdw.CLM_NBR where db2.clm_recept_dt 
BETWEEN :V_CLAIM_RECEIPT_START_DATE AND :V_CLAIM_RECEIPT_END_DATE
AND ISDW.CLM_PD_DT_ID BETWEEN :V_CLAIM_PAID_START_DATE  AND :V_CLAIM_PAID_END_DATE
;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_1)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);



   V_STEP := ''STEP2'';
   
   V_STEP_NAME := ''Load TMP_INST_CLAIM_TEMP_1'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


   


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_INST_CLAIM_TEMP_1) AS
SELECT DISTINCT clm.network_trace_number,
clm.health_care_code_info,
clm.other_diagnosis_cd_info,
clm.transactset_create_date,
clm.grp_control_no,
clm.trancactset_cntl_no,
clm.app_sender_code,
clm.clm_billing_note_text
FROM IDENTIFIER(:V_INST_CLAIM_PART) clm
WHERE clm.transactset_create_date BETWEEN 
:V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE ;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_INST_CLAIM_TEMP_1)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

   V_STEP := ''STEP3'';
   
   V_STEP_NAME := ''Load TMP_INST_MEMBER_TEMP'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_INST_MEMBER_TEMP) AS
SELECT DISTINCT mem.app_sender_code,
sub.mbi,
mem.subscriber_id,
mem.grp_control_no,
mem.trancactset_cntl_no,
mem.transactset_create_date
FROM IDENTIFIER(:V_INST_SUBSCRIBER)  mem
inner join IDENTIFIER(:V_SUBSCRIBER_INPUT)  sub on mem.subscriber_id = sub.subscriber_id where 
mem.transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE ;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_INST_MEMBER_TEMP)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

 V_STEP := ''STEP4'';
   
   V_STEP_NAME := ''Load TMP_INST_CLAIM_TEMP_NEW'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());




Create or replace temporary table IDENTIFIER(:V_TMP_INST_CLAIM_TEMP_NEW)
as
SELECT DISTINCT d.clm_num,
mem.mbi,
icd.icdcd,
clm.transactset_create_date
FROM IDENTIFIER(:V_INPUT_ICD) icd, IDENTIFIER(:V_TMP_INST_CLAIM_TEMP_1) clm
LEFT JOIN IDENTIFIER(:V_TMP_INST_MEMBER_TEMP) mem ON (mem.grp_control_no = clm.grp_control_no AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
AND mem.transactset_create_date = clm.transactset_create_date)
INNER JOIN IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_1) d ON clm.network_trace_number = d.clh_trk_id
AND YEAR(d.clm_recept_dt::date) = YEAR(to_date(clm.transactset_create_date,''YYYYMMDD''))
Where
(split(clm.health_care_code_info,'':'')[1] = icd.icdcd
OR
concat_ws('':'',
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[0]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[1]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[2]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[3]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[4]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[5]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[6]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[7]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[8]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[9]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[10]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[11]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[12]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[13]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[14]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[15]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[16]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[17]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[18]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[19]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[20]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[21]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[22]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[23]::string) like concat(''%:'',icd.icdcd,'':%''))


UNION


SELECT DISTINCT rpad(trim(substr(clm.clm_billing_note_text,2,12)),12,''1'') as clm_num,
mem.mbi,
icd.icdcd,
clm.transactset_create_date
FROM IDENTIFIER(:V_INPUT_ICD) icd,IDENTIFIER(:V_TMP_INST_CLAIM_TEMP_1) clm
LEFT JOIN IDENTIFIER(:V_TMP_INST_MEMBER_TEMP) mem ON (mem.grp_control_no = clm.grp_control_no
AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
AND mem.transactset_create_date = clm.transactset_create_date)
INNER JOIN IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_1) isdw on (isdw.CLM_NUM = rpad(trim(substr(clm.clm_billing_note_text,2,12)),12,''1''))
WHERE
--ICD filter
(split(clm.health_care_code_info,'':'')[1] = icd.icdcd
OR
concat_ws('':'',
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[0]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[1]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[2]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[3]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[4]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[5]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[6]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[7]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[8]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[9]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[10]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[11]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[12]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[13]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[14]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[15]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[16]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[17]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[18]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[19]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[20]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[21]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[22]::string,
PARSE_JSON(CLM.OTHER_DIAGNOSIS_CD_INFO)::ARRAY[23]::string) like concat(''%:'',icd.icdcd,'':%''))
AND clm.app_sender_code = ''EXELA'' AND mem.app_sender_code = ''EXELA'';

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_INST_CLAIM_TEMP_NEW)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 

V_STEP := ''STEP5'';
   
V_STEP_NAME := ''Load TMP_PROF_CLAIM_TEMP_1 '';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());




CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_PROF_CLAIM_TEMP_1) AS
SELECT DISTINCT clm.payer_clm_ctrl_num,
clm.network_trace_number,
clm.health_care_code_info,
clm.health_care_additional_code_info,
clm.vendor_cd,
clm.grp_control_no,
clm.trancactset_cntl_no,
clm.provider_hl_no,
clm.subscriber_hl_no,
clm.transactset_create_date
FROM IDENTIFIER(:V_PROF_CLAIM_PART) clm
WHERE clm.transactset_create_date BETWEEN :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE ;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PROF_CLAIM_TEMP_1)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

   V_STEP := ''STEP6'';
   
   V_STEP_NAME := ''Load PROF_MEMBER_TEMP'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());




CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_TMP_PROF_MEMBER_TEMP) AS
SELECT DISTINCT mem.app_sender_code,
sub.mbi,
mem.subscriber_id,
mem.grp_control_no,
mem.trancactset_cntl_no,
mem.subscriber_hl_no,
mem.transactset_create_date
FROM IDENTIFIER(:V_PROF_SUBSCRIBER) mem
inner join IDENTIFIER(:V_SUBSCRIBER_INPUT) sub on mem.subscriber_id = sub.subscriber_id where 
mem.transactset_create_date BETWEEN :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE ;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PROF_MEMBER_TEMP)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
                                 


   V_STEP := ''STEP7'';
   
   V_STEP_NAME := ''Load TMP_PROF_CLAIM_TEMP_NEW'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());



Create or replace temporary table IDENTIFIER(:V_TMP_PROF_CLAIM_TEMP_NEW) as 
SELECT DISTINCT d.clm_num,
mem.mbi,
icd.icdcd,
clm.transactset_create_date
FROM IDENTIFIER(:V_INPUT_ICD) icd,IDENTIFIER(:V_TMP_PROF_CLAIM_TEMP_1) clm
LEFT JOIN IDENTIFIER(:V_TMP_PROF_MEMBER_TEMP) mem ON (mem.grp_control_no = clm.grp_control_no AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
AND mem.subscriber_hl_no = clm.subscriber_hl_no
AND mem.transactset_create_date = clm.transactset_create_date)
INNER JOIN IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_1) d ON clm.payer_clm_ctrl_num = d.medicare_clm_cntrl_num
AND mem.subscriber_id = d.member_id
AND YEAR(d.clm_recept_dt::date) = YEAR(to_date(clm.transactset_create_date,''YYYYMMDD''))
WHERE clm.vendor_cd = ''CMS'' and
--ICD filter
(split(clm.health_care_code_info,'':'')[1] = icd.icdcd
OR
concat_ws('':'',
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[0]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[1]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[2]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[3]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[4]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[5]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[6]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[7]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[8]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[9]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[10]::string) like concat(''%:'',icd.icdcd,'':%''))
UNION
SELECT DISTINCT d.clm_num,
mem.mbi,
icd.icdcd,
clm.transactset_create_date
FROM  IDENTIFIER(:V_INPUT_ICD) icd, IDENTIFIER(:V_TMP_PROF_CLAIM_TEMP_1) clm
LEFT JOIN IDENTIFIER(:V_TMP_PROF_MEMBER_TEMP) mem ON (mem.grp_control_no = clm.grp_control_no
AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
AND mem.transactset_create_date = clm.transactset_create_date)
INNER JOIN IDENTIFIER(:V_TMP_DB2IMPORT_CLM_FILTERED_1) d ON clm.network_trace_number = d.clh_trk_id
AND YEAR(d.clm_recept_dt::date) = YEAR(to_date(clm.transactset_create_date,''YYYYMMDD''))
WHERE clm.vendor_cd = ''CH'' and
--ICD filter
(split(clm.health_care_code_info,'':'')[1] = icd.icdcd
OR
concat_ws('':'',
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[0]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[1]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[2]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[3]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[4]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[5]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[6]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[7]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[8]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[9]::string,
PARSE_JSON(CLM.health_care_additional_code_info)::ARRAY[10]::string) like concat(''%:'',icd.icdcd,'':%''));

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_PROF_CLAIM_TEMP_NEW)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 


   V_STEP := ''STEP8'';
   
   V_STEP_NAME := ''Load TMP_OUTPUT_COMBINED'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());




Create or replace temporary table IDENTIFIER(:V_TMP_OUTPUT_COMBINED) as
select * from IDENTIFIER(:V_TMP_INST_CLAIM_TEMP_NEW) where mbi is not null
union
select * from IDENTIFIER(:V_TMP_PROF_CLAIM_TEMP_NEW) where mbi is not null;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_OUTPUT_COMBINED)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);



   V_STEP := ''STEP9'';
   
   V_STEP_NAME := ''Load TMP_FINAL_OUTPUT_COMBINED'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());



Create or replace temporary table IDENTIFIER(:V_TMP_FINAL_OUTPUT_COMBINED)
as
SELECT DISTINCT inst.mbi,
''02'' as Qualifier,
''1'' as Type_Code,
inst.icdcd,
isdw.SRVC_FROM_DT_ID,
''20391231'' as SRVC_TO_DT_ID
from IDENTIFIER(:V_TMP_OUTPUT_COMBINED) inst
join IDENTIFIER(:V_ISDW_SERVICE_DATES_IMPORT) isdw on inst.clm_num = isdw.CLM_NBR;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_TMP_FINAL_OUTPUT_COMBINED)) ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
V_STEP := ''STEP10'';

 
V_STEP_NAME := ''Generate Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());                                 

V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/mspdp/''||''BK1P_ICD10_''||(select TO_VARCHAR(current_date()::date,''YYYYMMDD''))||''.csv''||'' FROM (
             
             
             
             SELECT distinct *
               FROM TMP_FINAL_OUTPUT_COMBINED
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
