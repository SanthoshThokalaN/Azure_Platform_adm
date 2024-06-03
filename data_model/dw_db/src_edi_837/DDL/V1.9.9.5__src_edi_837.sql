USE SCHEMA SRC_EDI_837;


CREATE OR REPLACE PROCEDURE SP_LEGACY_INST_PROVIDER_ALL("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "TRAN_MTH" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "SRC_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME   VARCHAR DEFAULT ''LEGACY_LOAD'';

V_SUB_PROCESS_NAME  VARCHAR DEFAULT  ''LEGACY_INST_PROVIDER_ALL'';

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

V_TEMP_TBL  VARCHAR :=  :SRC_SC||''.''||''TMP_INST_PROVIDER_ALL_RAW''||''_''||:TRAN_MTH;

V_LZ_TBL    VARCHAR :=  :SRC_SC||''.''||''INST_PROVIDER_ALL_RAW'';

V_SRC_TBL   VARCHAR :=  :TGT_SC||''.''||''INST_PROVIDER_ALL'';
 

BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP1'';


V_STEP_NAME := ''INST_PROVIDER_ALL''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());


V_QUERY1 := ''CREATE OR REPLACE TEMPORARY TABLE ''||:V_TEMP_TBL||'' AS (SELECT * FROM ''||V_LZ_TBL||'' WHERE 1 = 2); '';  
 
V_QUERY2 := '' 

COPY INTO ''||:V_TEMP_TBL||'' FROM  (
select 
$1 app_sender_code,
        $2 app_reciever_code,
        $3 grp_control_no,
        $4 trancactset_cntl_no,
        $5 impl_convention_refer,
        $6 transactset_purpose_code,
        $7 batch_cntl_no,
        $8 transactset_create_time,
        $9 transact_type_code,
        $10 claim_id,
        STRTOK_TO_ARRAY(replace($11,'''''''',''''~''''),'''''''') clm_rendering_prv_map,
        STRTOK_TO_ARRAY(replace($12,'''''''',''''~''''),'''''''') clm_attending_prv_map,
        STRTOK_TO_ARRAY(replace($13,'''''''',''''~''''),'''''''') clm_referring_prv_map,
        STRTOK_TO_ARRAY(replace($14,'''''''',''''~''''),'''''''') clm_operating_phys_map,
        STRTOK_TO_ARRAY(replace($15,'''''''',''''~''''),'''''''') clm_other_operating_phys_map,
        $16 sv_lx_number,
        STRTOK_TO_ARRAY(replace($17,'''''''',''''~''''),'''''''') sv_rendering_prv_map,
        STRTOK_TO_ARRAY(replace($18,'''''''',''''~''''),'''''''') sv_referring_prv_map,
        STRTOK_TO_ARRAY(replace($19,'''''''',''''~''''),'''''''') sv_operating_phys_map,
        STRTOK_TO_ARRAY(replace($20,'''''''',''''~''''),'''''''') sv_other_operating_phys_map,
metadata$filename
from ''||:V_STAGE||'' ) file_format = ''''UTIL.FF_SOH_CSV'''', pattern=''''.*transactset_create_date=.*.000.*'''' ;''
;

execute immediate ''USE SCHEMA ''||:TGT_SC; 
execute immediate :V_QUERY1;  
execute immediate :V_QUERY2; 


CREATE TABLE IF NOT EXISTS LZ_EDI_837.INST_PROVIDER_ALL_ERROR AS SELECT * FROM LZ_EDI_837.INST_PROVIDER_ALL_RAW WHERE 1 = 2;

INSERT INTO LZ_EDI_837.INST_PROVIDER_ALL_ERROR 
SELECT * FROM IDENTIFIER(:V_TEMP_TBL)
WHERE sv_lx_number LIKE ''%\N%''
;


INSERT INTO IDENTIFIER(:V_SRC_TBL)
(
app_sender_code,
app_reciever_code,
grp_control_no,
trancactset_cntl_no,
impl_convention_refer,
transactset_purpose_code,
batch_cntl_no,
transactset_create_time,
transact_type_code,
CLAIM_ID,
 clm_rendering_prv_map,
clm_attending_prv_map, 
 clm_referring_prv_map,
 clm_operating_phys_map, 
 clm_other_operating_phys_map, 
 SV_LX_NUMBER,
sv_rendering_prv_map,
 sv_referring_prv_map,
 sv_operating_phys_map,
 sv_other_operating_phys_map,
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
app_sender_code,
        app_reciever_code,
        grp_control_no,
        trancactset_cntl_no,
        impl_convention_refer,
        transactset_purpose_code,
        batch_cntl_no,
        transactset_create_time,
        transact_type_code,
        CLAIM_ID,
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
        CASE WHEN clm_attending_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_attending_prv_map[0]::VARCHAR  ,replace(clm_attending_prv_map[1]::VARCHAR,''~'')  ,
                         clm_attending_prv_map[2]::VARCHAR  ,replace(clm_attending_prv_map[3]::VARCHAR,''~'')  ,
                         clm_attending_prv_map[4]::VARCHAR  ,replace(clm_attending_prv_map[5]::VARCHAR,''~'')  ,
                         clm_attending_prv_map[6]::VARCHAR  ,replace(clm_attending_prv_map[7]::VARCHAR,''~'')  ,
                         clm_attending_prv_map[8]::VARCHAR  ,replace(clm_attending_prv_map[9]::VARCHAR,''~'')  ,
                         clm_attending_prv_map[10]::VARCHAR ,replace(clm_attending_prv_map[11]::VARCHAR,''~'') ,
                         clm_attending_prv_map[12]::VARCHAR ,replace(clm_attending_prv_map[13]::VARCHAR,''~'') ,
                         clm_attending_prv_map[14]::VARCHAR ,replace(clm_attending_prv_map[15]::VARCHAR,''~'') ) END clm_attending_prv_map, 
        CASE WHEN clm_referring_prv_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_referring_prv_map[0]::VARCHAR  ,replace(clm_referring_prv_map[1]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[2]::VARCHAR  ,replace(clm_referring_prv_map[3]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[4]::VARCHAR  ,replace(clm_referring_prv_map[5]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[6]::VARCHAR  ,replace(clm_referring_prv_map[7]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[8]::VARCHAR  ,replace(clm_referring_prv_map[9]::VARCHAR,''~'')  ,
                         clm_referring_prv_map[10]::VARCHAR ,replace(clm_referring_prv_map[11]::VARCHAR,''~'') ,
                         clm_referring_prv_map[12]::VARCHAR ,replace(clm_referring_prv_map[13]::VARCHAR,''~'') ,
                         clm_referring_prv_map[14]::VARCHAR ,replace(clm_referring_prv_map[15]::VARCHAR,''~'') ) END clm_referring_prv_map,

        CASE WHEN clm_operating_phys_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_operating_phys_map[0]::VARCHAR  ,replace(clm_operating_phys_map[1]::VARCHAR,''~'')  ,
                         clm_operating_phys_map[2]::VARCHAR  ,replace(clm_operating_phys_map[3]::VARCHAR,''~'')  ,
                         clm_operating_phys_map[4]::VARCHAR  ,replace(clm_operating_phys_map[5]::VARCHAR,''~'')  ,
                         clm_operating_phys_map[6]::VARCHAR  ,replace(clm_operating_phys_map[7]::VARCHAR,''~'')  ,
                         clm_operating_phys_map[8]::VARCHAR  ,replace(clm_operating_phys_map[9]::VARCHAR,''~'')  ,
                         clm_operating_phys_map[10]::VARCHAR ,replace(clm_operating_phys_map[11]::VARCHAR,''~'') ,
                         clm_operating_phys_map[12]::VARCHAR ,replace(clm_operating_phys_map[13]::VARCHAR,''~'') ,
                         clm_operating_phys_map[14]::VARCHAR ,replace(clm_operating_phys_map[15]::VARCHAR,''~'') ) END clm_operating_phys_map, 

        CASE WHEN clm_other_operating_phys_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE 
        OBJECT_CONSTRUCT(clm_other_operating_phys_map[0]::VARCHAR  ,replace(clm_other_operating_phys_map[1]::VARCHAR,''~'')  ,
                         clm_other_operating_phys_map[2]::VARCHAR  ,replace(clm_other_operating_phys_map[3]::VARCHAR,''~'')  ,
                         clm_other_operating_phys_map[4]::VARCHAR  ,replace(clm_other_operating_phys_map[5]::VARCHAR,''~'')  ,
                         clm_other_operating_phys_map[6]::VARCHAR  ,replace(clm_other_operating_phys_map[7]::VARCHAR,''~'')  ,
                         clm_other_operating_phys_map[8]::VARCHAR  ,replace(clm_other_operating_phys_map[9]::VARCHAR,''~'')  ,
                         clm_other_operating_phys_map[10]::VARCHAR ,replace(clm_other_operating_phys_map[11]::VARCHAR,''~'') ,
                         clm_other_operating_phys_map[12]::VARCHAR ,replace(clm_other_operating_phys_map[13]::VARCHAR,''~'') ,
                         clm_other_operating_phys_map[14]::VARCHAR ,replace(clm_other_operating_phys_map[15]::VARCHAR,''~'') ) END clm_other_operating_phys_map, 

        sv_lx_number,
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
        CASE WHEN sv_operating_phys_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE                  
        OBJECT_CONSTRUCT(sv_operating_phys_map[0]::VARCHAR  ,replace(sv_operating_phys_map[1]::VARCHAR,''~'')  ,
                         sv_operating_phys_map[2]::VARCHAR  ,replace(sv_operating_phys_map[3]::VARCHAR,''~'')  ,
                         sv_operating_phys_map[4]::VARCHAR  ,replace(sv_operating_phys_map[5]::VARCHAR,''~'')  ,
                         sv_operating_phys_map[6]::VARCHAR  ,replace(sv_operating_phys_map[7]::VARCHAR,''~'')  ,
                         sv_operating_phys_map[8]::VARCHAR  ,replace(sv_operating_phys_map[9]::VARCHAR,''~'')  ,
                         sv_operating_phys_map[10]::VARCHAR ,replace(sv_operating_phys_map[11]::VARCHAR,''~'') ,
                         sv_operating_phys_map[12]::VARCHAR ,replace(sv_operating_phys_map[13]::VARCHAR,''~'') ,
                         sv_operating_phys_map[14]::VARCHAR ,replace(sv_operating_phys_map[15]::VARCHAR,''~'') ,
                         sv_operating_phys_map[16]::VARCHAR ,replace(sv_operating_phys_map[17]::VARCHAR,''~'') ) END sv_operating_phys_map,
        CASE WHEN sv_other_operating_phys_map[0]::VARCHAR=''\\\\N'' THEN OBJECT_CONSTRUCT(null,null) ELSE                  
        OBJECT_CONSTRUCT(sv_other_operating_phys_map[0]::VARCHAR  ,replace(sv_other_operating_phys_map[1]::VARCHAR,''~'')  ,
                         sv_other_operating_phys_map[2]::VARCHAR  ,replace(sv_other_operating_phys_map[3]::VARCHAR,''~'')  ,
                         sv_other_operating_phys_map[4]::VARCHAR  ,replace(sv_other_operating_phys_map[5]::VARCHAR,''~'')  ,
                         sv_other_operating_phys_map[6]::VARCHAR  ,replace(sv_other_operating_phys_map[7]::VARCHAR,''~'')  ,
                         sv_other_operating_phys_map[8]::VARCHAR  ,replace(sv_other_operating_phys_map[9]::VARCHAR,''~'')  ,
                         sv_other_operating_phys_map[10]::VARCHAR ,replace(sv_other_operating_phys_map[11]::VARCHAR,''~'') ,
                         sv_other_operating_phys_map[12]::VARCHAR ,replace(sv_other_operating_phys_map[13]::VARCHAR,''~'') ,
                         sv_other_operating_phys_map[14]::VARCHAR ,replace(sv_other_operating_phys_map[15]::VARCHAR,''~'') ,
                         sv_other_operating_phys_map[16]::VARCHAR ,replace(sv_other_operating_phys_map[17]::VARCHAR,''~'') ) END sv_other_operating_phys_map,
  SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8) as transactset_create_date,
NULL as CLAIM_TRACKING_ID, 
NULL   AS XML_MD5,
MD5(APP_SENDER_CODE||APP_RECIEVER_CODE||GRP_CONTROL_NO||TRANCACTSET_CNTL_NO||IMPL_CONVENTION_REFER
||TRANSACTSET_PURPOSE_CODE||BATCH_CNTL_NO||SUBSTR(transactset_create_date, REGEXP_INSTR(transactset_create_date, ''transactset_create_date='')+24, 8)||TRANSACTSET_CREATE_TIME||TRANSACT_TYPE_CODE) AS XML_HDR_MD5,
''LEGACY'' AS FILE_SOURCE, 
null as FILE_NAME, 
CURRENT_TIMESTAMP, 
CURRENT_TIMESTAMP

FROM IDENTIFIER(:V_TEMP_TBL)
WHERE sv_lx_number NOT LIKE ''%\N%''

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
