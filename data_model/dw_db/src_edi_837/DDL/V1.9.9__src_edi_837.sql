USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_LEGACY_PROF_PROVIDER_ALL("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_PROF_PROVIDER_ALL'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE VARCHAR := :STAGE||''/transactset_create_date=''||:TRAN_MTH;

V_QUERY1 VARCHAR;

V_QUERY2 VARCHAR;

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_PROF_PROVIDER_ALL_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''PROF_PROVIDER_ALL_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''PROF_PROVIDER_ALL'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''PROF_PROVIDER_ALL''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS (SELECT * FROM ''||V_LZ_TBL||'' WHERE 1 = 2); '';  
 
V_QUERY2 := '' 

COPY INTO ''||:V_TEMP_TBL||'' FROM  (
select 
$1 ,
        $2 ,
        $3 ,
        $4 ,
        $5 ,
        $6 ,
        $7 ,
        $8 ,
        $9 ,
        $10 ,
        $11 ,
        $12 ,
        $13 ,
        STRTOK_TO_ARRAY(replace($14,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($15,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($16,'''''''',''''~''''),'''''''') ,
        $17 ,
        STRTOK_TO_ARRAY(replace($18,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($19,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($20,'''''''',''''~''''),'''''''') ,
        STRTOK_TO_ARRAY(replace($21,'''''''',''''~''''),'''''''') ,
metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_SOH_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 

CREATE TABLE IF NOT EXISTS LZ_EDI_837.PROF_PROVIDER_ALL_ERROR AS SELECT * FROM LZ_EDI_837.PROF_PROVIDER_ALL_RAW WHERE 1 = 2;

INSERT INTO LZ_EDI_837.PROF_PROVIDER_ALL_ERROR 
SELECT * FROM IDENTIFIER(:V_TEMP_TBL)
WHERE SL_SEQ_NUM LIKE ''%\N%''
;


INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
  app_sender_code , 
  app_reciever_code , 
  grp_control_no , 
  trancactset_cntl_no , 
  impl_convention_refer , 
  transactset_purpose_code , 
  batch_cntl_no , 
  transactset_create_time , 
  transact_type_code , 
  provider_hl_no , 
  subscriber_hl_no , 
  payer_hl_no , 
  claim_id , 
  clm_rendering_prv_map , 
  clm_supervising_prv_map , 
  clm_referring_prv_map , 
  sl_seq_num , 
  sv_rendering_prv_map , 
  sv_supervising_prv_map , 
  sv_referring_prv_map , 
  sv_ordering_prv_map ,
  transactset_create_date,
  CLAIM_TRACKING_ID,
  XML_MD5,
  XML_HDR_MD5,
  FILE_SOURCE,
  FILE_NAME,
  ISDC_CREATED_DT,
  ISDC_UPDATED_DT
)
 
SELECT DISTINCT   
  app_sender_code , 
  app_reciever_code , 
  grp_control_no , 
  trancactset_cntl_no , 
  impl_convention_refer , 
  transactset_purpose_code , 
  batch_cntl_no , 
  transactset_create_time , 
  transact_type_code , 
  provider_hl_no , 
  subscriber_hl_no , 
  payer_hl_no , 
  claim_id , 
CASE WHEN clm_rendering_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_rendering_prv_map[0]::VARCHAR  ,replace(clm_rendering_prv_map[1]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[2]::VARCHAR  ,replace(clm_rendering_prv_map[3]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[4]::VARCHAR  ,replace(clm_rendering_prv_map[5]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[6]::VARCHAR  ,replace(clm_rendering_prv_map[7]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[8]::VARCHAR  ,replace(clm_rendering_prv_map[9]::VARCHAR,''~'')  ,
                         clm_rendering_prv_map[10]::VARCHAR ,replace(clm_rendering_prv_map[11]::VARCHAR,''~'') ,
                         clm_rendering_prv_map[12]::VARCHAR ,replace(clm_rendering_prv_map[13]::VARCHAR,''~'') ,
                         clm_rendering_prv_map[14]::VARCHAR ,replace(clm_rendering_prv_map[15]::VARCHAR,''~'') ,
                         clm_rendering_prv_map[16]::VARCHAR ,NULLIF(replace(clm_rendering_prv_map[17]::VARCHAR,''~''),''\\\\N'') ,
                         clm_rendering_prv_map[18]::VARCHAR ,NULLIF(replace(clm_rendering_prv_map[19]::VARCHAR,''~''),''\\\\N'') ,
                         clm_rendering_prv_map[20]::VARCHAR ,replace(clm_rendering_prv_map[21]::VARCHAR,''~'')) END clm_rendering_prv_map, 
  CASE WHEN clm_supervising_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_supervising_prv_map[0]::VARCHAR  ,replace(clm_supervising_prv_map[1]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[2]::VARCHAR  ,replace(clm_supervising_prv_map[3]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[4]::VARCHAR  ,replace(clm_supervising_prv_map[5]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[6]::VARCHAR  ,replace(clm_supervising_prv_map[7]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[8]::VARCHAR  ,replace(clm_supervising_prv_map[9]::VARCHAR,''~'')  ,
                         clm_supervising_prv_map[10]::VARCHAR ,replace(clm_supervising_prv_map[11]::VARCHAR,''~'') ,
                         clm_supervising_prv_map[12]::VARCHAR ,replace(clm_supervising_prv_map[13]::VARCHAR,''~'') ,
                         clm_supervising_prv_map[14]::VARCHAR ,replace(clm_supervising_prv_map[15]::VARCHAR,''~'') ) END clm_supervising_prv_map,  
  CASE WHEN clm_referring_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_referring_prv_map[0]::VARCHAR  ,replace(clm_referring_prv_map[1]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[2]::VARCHAR  ,replace(clm_referring_prv_map[3]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[4]::VARCHAR  ,replace(clm_referring_prv_map[5]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[6]::VARCHAR  ,replace(clm_referring_prv_map[7]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[8]::VARCHAR  ,replace(clm_referring_prv_map[9]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[10]::VARCHAR ,replace(clm_referring_prv_map[11]::VARCHAR,''~'') ,
                         clm_referring_prv_map[12]::VARCHAR ,replace(clm_referring_prv_map[13]::VARCHAR,''~'') ,
                         clm_referring_prv_map[14]::VARCHAR ,replace(clm_referring_prv_map[15]::VARCHAR,''~'') ) END clm_referring_prv_map,   
  sl_seq_num , 
  CASE WHEN sv_rendering_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(sv_rendering_prv_map[0]::VARCHAR  ,replace(sv_rendering_prv_map[1]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[2]::VARCHAR  ,replace(sv_rendering_prv_map[3]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[4]::VARCHAR  ,replace(sv_rendering_prv_map[5]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[6]::VARCHAR  ,replace(sv_rendering_prv_map[7]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[8]::VARCHAR  ,replace(sv_rendering_prv_map[9]::VARCHAR,''~'')  ,
                         sv_rendering_prv_map[10]::VARCHAR ,replace(sv_rendering_prv_map[11]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[12]::VARCHAR ,replace(sv_rendering_prv_map[13]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[14]::VARCHAR ,replace(sv_rendering_prv_map[15]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[16]::VARCHAR ,replace(sv_rendering_prv_map[17]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[18]::VARCHAR ,replace(sv_rendering_prv_map[19]::VARCHAR,''~'') ,
                         sv_rendering_prv_map[20]::VARCHAR ,replace(sv_rendering_prv_map[21]::VARCHAR,''~'') ) END sv_rendering_prv_map, 
  CASE WHEN sv_supervising_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE                  
        OBJECT_CONSTRUCT(sv_supervising_prv_map[0]::VARCHAR  ,replace(sv_supervising_prv_map[1]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[2]::VARCHAR  ,replace(sv_supervising_prv_map[3]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[4]::VARCHAR  ,replace(sv_supervising_prv_map[5]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[6]::VARCHAR  ,replace(sv_supervising_prv_map[7]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[8]::VARCHAR  ,replace(sv_supervising_prv_map[9]::VARCHAR,''~'')  ,
                         sv_supervising_prv_map[10]::VARCHAR ,replace(sv_supervising_prv_map[11]::VARCHAR,''~'') ,
                         sv_supervising_prv_map[12]::VARCHAR ,replace(sv_supervising_prv_map[13]::VARCHAR,''~'') ,
                         sv_supervising_prv_map[14]::VARCHAR ,replace(sv_supervising_prv_map[15]::VARCHAR,''~'') ,
                         sv_supervising_prv_map[16]::VARCHAR ,replace(sv_supervising_prv_map[17]::VARCHAR,''~'') ) END sv_supervising_prv_map, 
  CASE WHEN sv_referring_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE                  
        OBJECT_CONSTRUCT(sv_referring_prv_map[0]::VARCHAR  ,replace(sv_referring_prv_map[1]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[2]::VARCHAR  ,replace(sv_referring_prv_map[3]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[4]::VARCHAR  ,replace(sv_referring_prv_map[5]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[6]::VARCHAR  ,replace(sv_referring_prv_map[7]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[8]::VARCHAR  ,replace(sv_referring_prv_map[9]::VARCHAR,''~'')  ,
                         sv_referring_prv_map[10]::VARCHAR ,replace(sv_referring_prv_map[11]::VARCHAR,''~'') ,
                         sv_referring_prv_map[12]::VARCHAR ,replace(sv_referring_prv_map[13]::VARCHAR,''~'') ,
                         sv_referring_prv_map[14]::VARCHAR ,replace(sv_referring_prv_map[15]::VARCHAR,''~'') ,
                         sv_referring_prv_map[16]::VARCHAR ,replace(sv_referring_prv_map[17]::VARCHAR,''~'') ) END sv_referring_prv_map, 
  CASE WHEN sv_ordering_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE                  
        OBJECT_CONSTRUCT(sv_ordering_prv_map[0]::VARCHAR  ,replace(sv_ordering_prv_map[1]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[2]::VARCHAR  ,replace(sv_ordering_prv_map[3]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[4]::VARCHAR  ,replace(sv_ordering_prv_map[5]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[6]::VARCHAR  ,replace(sv_ordering_prv_map[7]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[8]::VARCHAR  ,replace(sv_ordering_prv_map[9]::VARCHAR,''~'')  ,
                         sv_ordering_prv_map[10]::VARCHAR ,replace(sv_ordering_prv_map[11]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[12]::VARCHAR ,replace(sv_ordering_prv_map[13]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[14]::VARCHAR ,replace(sv_ordering_prv_map[15]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[16]::VARCHAR ,replace(sv_ordering_prv_map[17]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[18]::VARCHAR ,replace(sv_ordering_prv_map[19]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[20]::VARCHAR ,replace(sv_ordering_prv_map[21]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[22]::VARCHAR ,replace(sv_ordering_prv_map[23]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[24]::VARCHAR ,replace(sv_ordering_prv_map[25]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[26]::VARCHAR ,replace(sv_ordering_prv_map[27]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[28]::VARCHAR ,replace(sv_ordering_prv_map[29]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[30]::VARCHAR ,replace(sv_ordering_prv_map[31]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[32]::VARCHAR ,replace(sv_ordering_prv_map[33]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[34]::VARCHAR ,replace(sv_ordering_prv_map[35]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[36]::VARCHAR ,replace(sv_ordering_prv_map[37]::VARCHAR,''~'') ,
                         sv_ordering_prv_map[38]::VARCHAR ,replace(sv_ordering_prv_map[39]::VARCHAR,''~'') ) END sv_ordering_prv_map,
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) AS transactset_create_date ,
NULL as CLAIM_TRACKING_ID, 


MD5(GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||(SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8))||CLAIM_ID)   AS XML_MD5,


MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)
WHERE SL_SEQ_NUM NOT LIKE ''%\N%''

;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :TRAN_MTH, NULL, NULL);

UPDATE UTIL.LEGACY_LOAD SET LOAD_INDC = ''Y'' WHERE TABLE_NAME = :V_SUB_PROCESS_NAME AND TRAN_MTH = :TRAN_MTH;

EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
