USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_INST_CLM_SV_DATES"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''INST_CLM_SV_DATES'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_INST_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.INST_CLAIMS_RAW'';

V_INST_CLM_SV_DATES VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.INST_CLM_SV_DATES'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD INST_CLM_SV_DATES'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_INST_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );

INSERT INTO IDENTIFIER(:V_INST_CLM_SV_DATES)
(
  APP_SENDER_CODE , 
  APP_RECIEVER_CODE , 
  GRP_CONTROL_NO , 
  TRANCACTSET_CNTL_NO , 
  IMPL_CONVENTION_REFER , 
  TRANSACTSET_PURPOSE_CODE , 
  BATCH_CNTL_NO , 
  TRANSACTSET_CREATE_TIME , 
  TRANSACT_TYPE_CODE , 
  CLAIM_ID , 
  SL_SEQ_NUM , 
  CLM_DATE_TYPE , 
  CLM_DATE_FORMAT_QLFR , 
  CLM_DATE ,
  TRANSACTSET_CREATE_DATE , 
  CLAIM_TRACKING_ID ,
  XML_MD5 ,
  XML_HDR_MD5 ,
  FILE_SOURCE ,
  FILE_NAME
)   
     
WITH 


RAW AS (
SELECT * FROM IDENTIFIER(:V_INST_CLAIMS_RAW) R WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_INST_CLM_SV_DATES) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
QUALIFY ROW_NUMBER() OVER  (PARTITION BY XML_MD5 ORDER BY 1) = 1

)
,


segments as (
select   XML_MD5, value as XML, SUBSTR(VALUE, 2, regexp_instr(VALUE, ''>'')-2) AS segment, FILE_SOURCE, FILE_NAME, row_number() OVER (PARTITION BY XML_MD5, SUBSTR(VALUE, 2, regexp_instr(VALUE, ''>'')-2) ORDER BY 1 ) AS IDX
from
RAW P,
lateral FLATTEN(P.xml:"$") 
)

,

claimTrackingId AS (
select 
XML_MD5, replace( GET( XML, ''$''), ''\\"'','''') AS claimTrackingId, FILE_SOURCE, FILE_NAME
from segments
where segment = ''claimTrackingId''
)
,

GS AS (
SELECT 
XML_MD5, 
XMLGET( XML,''appSenderCode''):"$"::VARCHAR  AS app_sender_code,
XMLGET( XML,''appReceiverCode''):"$"::VARCHAR  AS app_reciever_code,
XMLGET( XML,''grpControlNumber''):"$"::INTEGER  AS grp_control_no

FROM SEGMENTS
WHERE SEGMENT = ''GS''
)
,

ST AS (
SELECT 
XML_MD5, 
XMLGET( XML,''transactSetControlNumber''):"$"::VARCHAR  As trancactset_cntl_no,
XMLGET( XML,''implConventionRefer''):"$"::VARCHAR  As impl_convention_refer
FROM SEGMENTS
WHERE SEGMENT = ''ST''
)
,

BHT AS 
(
SELECT 
XML_MD5, 
XMLGET( XML,''transactSetPurposeCode''):"$"::INTEGER  As transactset_purpose_code,
XMLGET( XML,''batchControlNumber''):"$"::VARCHAR  As batch_cntl_no,
XMLGET( XML,''transactSetCreateDate''):"$"::VARCHAR  As transactset_create_date,
XMLGET( XML,''transactSetCreateTime''):"$"::INTEGER  As transactset_create_time,
XMLGET( XML,''transactTypeCode''):"$"::VARCHAR  As transact_type_code
FROM SEGMENTS
WHERE SEGMENT = ''BHT''
)
,


Loop2300_CLM AS 
(
select 
XML_MD5, 
XMLGET ( CLM.value,''patControlNumber'' ): "$"::VARCHAR AS claim_id
from segments ,  LATERAL FLATTEN(to_array(GET(xml,''$''))) CLM
where segment = ''Loop2300''
and GET(CLM.value, ''@'') = ''CLM''
)

,

Loop2400_LX AS 
(
select 
XML_MD5, 
XMLGET ( LX.value,''svSeqNumber'' ): "$"::VARCHAR AS sl_seq_num
from segments,  LATERAL FLATTEN(to_array(GET(xml,''$''))) LX
where segment = ''Loop2400''
and GET(LX.value, ''@'') = ''LX''
)

,

Loop2400_DTP_472 AS 
(
select 
XML_MD5, 
XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
''472'' as clm_date_type,
XMLGET ( DTP_472.value,''svDateFormatQlfr'' ): "$"::VARCHAR AS clm_date_format_qlfr,
XMLGET ( DTP_472.value,''svDate'' ): "$"::VARCHAR AS clm_date
from segments,  LATERAL FLATTEN(to_array(GET(xml,''$''))) DTP_472
where segment = ''Loop2400''
and GET(DTP_472.value, ''@'') = ''DTP_472''
)

,



Loop2400_Loop2430_DTP_573 AS 
(
    select 
    XML_MD5,XMLGET( Loop2430.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    ''573'' as clm_date_type,
    XMLGET( DTP_573.value, ''svDateFormatQlfr'' ):"$"::VARCHAR  As clm_date_format_qlfr,
    XMLGET( DTP_573.value, ''svDate'' ):"$"::VARCHAR  As clm_date

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2430,  LATERAL FLATTEN(to_array(GET(Loop2430.value,''$''))) DTP_573
    where segment = ''Loop2400''
    and GET( Loop2430.value, ''@'') = ''Loop2430''
    and GET( DTP_573.VALUE, ''@'') = ''DTP_573''
)


, 

Loop2400_DTP AS (

select XML_MD5, sl_seq_num, clm_date_type, clm_date_format_qlfr, clm_date from  Loop2400_DTP_472
UNION ALL
select XML_MD5, sl_seq_num, clm_date_type, clm_date_format_qlfr, clm_date from  Loop2400_Loop2430_DTP_573 
)


,


Loop2300_DTP_096 AS 
(
select 
XML_MD5, 
 ''096'' as clm_date_type,
XMLGET ( DTP_431.value,''dateFormatQlfr'' ): "$"::VARCHAR AS clm_date_format_qlfr,
XMLGET ( DTP_431.value,''date'' ): "$"::VARCHAR AS clm_date
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) DTP_431
where segment = ''Loop2300''
and GET(DTP_431.value, ''@'') = ''DTP_096''
)

,

Loop2300_DTP_434 AS 
(
select 
XML_MD5, 
 ''434'' as clm_date_type,
XMLGET ( DTP_454.value,''dateFormatQlfr'' ): "$"::VARCHAR AS clm_date_format_qlfr,
XMLGET ( DTP_454.value,''date'' ): "$"::VARCHAR AS clm_date
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) DTP_454
where segment = ''Loop2300''
and GET(DTP_454.value, ''@'') = ''DTP_434''
)

,

Loop2300_DTP_435 AS 
(
select 
XML_MD5, 
 ''435'' as clm_date_type,
XMLGET ( DTP_304.value,''dateFormatQlfr'' ): "$"::VARCHAR AS clm_date_format_qlfr,
XMLGET ( DTP_304.value,''date'' ): "$"::VARCHAR AS clm_date
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) DTP_304
where segment = ''Loop2300''
and GET(DTP_304.value, ''@'') = ''DTP_435''
)

,

Loop2300_DTP_050 AS 
(
select 
XML_MD5, 
 ''050'' as clm_date_type,
XMLGET ( DTP_453.value,''dateFormatQlfr'' ): "$"::VARCHAR AS clm_date_format_qlfr,
XMLGET ( DTP_453.value,''date'' ): "$"::VARCHAR AS clm_date
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) DTP_453
where segment = ''Loop2300''
and GET(DTP_453.value, ''@'') = ''DTP_050''
)


,

Loop2320_Loop2330B_DTP_573 AS 
(
    select 
    XML_MD5,
    ''573'' as clm_date_type,
    XMLGET( DTP_573.value, ''dateFormatQlfr'' ):"$"::VARCHAR  As clm_date_format_qlfr,
    XMLGET( DTP_573.value, ''date'' ):"$"::VARCHAR  As clm_date
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2330B,  LATERAL FLATTEN(to_array(GET(Loop2330B.value,''$''))) DTP_573
    where segment = ''Loop2320''
    and GET( Loop2330B.value, ''@'') = ''Loop2330B''
    and GET( DTP_573.VALUE, ''@'') = ''DTP_573''
)

,


Loop2300_DTP AS 
(
select XML_MD5, clm_date_type, clm_date_format_qlfr, clm_date from  Loop2300_DTP_096
UNION ALL
select XML_MD5, clm_date_type, clm_date_format_qlfr, clm_date from  Loop2300_DTP_434
UNION ALL
select XML_MD5, clm_date_type, clm_date_format_qlfr, clm_date from  Loop2300_DTP_435
UNION ALL
select XML_MD5, clm_date_type, clm_date_format_qlfr, clm_date from  Loop2300_DTP_050
UNION ALL
select XML_MD5, clm_date_type, clm_date_format_qlfr, clm_date from  Loop2320_Loop2330B_DTP_573
)


select 

distinct 
GS.APP_SENDER_CODE,
GS.APP_RECIEVER_CODE,
GS.GRP_CONTROL_NO,
-------------
ST.TRANCACTSET_CNTL_NO,
ST.IMPL_CONVENTION_REFER,
-------------
BHT.TRANSACTSET_PURPOSE_CODE,
BHT.BATCH_CNTL_NO,

BHT.TRANSACTSET_CREATE_TIME,
BHT.TRANSACT_TYPE_CODE,
-------------

Loop2300_CLM.CLAIM_ID,
----
null as SL_SEQ_NUM,

---------
Loop2300_DTP.CLM_DATE_TYPE, 
Loop2300_DTP.CLM_DATE_FORMAT_QLFR, 
Loop2300_DTP.CLM_DATE,
------------
BHT.TRANSACTSET_CREATE_DATE,
----
  
claimTrackingId.claimTrackingId,  
claimTrackingId.XML_MD5,  
 MD5(GS.APP_SENDER_CODE||GS.APP_RECIEVER_CODE||GS.GRP_CONTROL_NO||ST.TRANCACTSET_CNTL_NO||ST.IMPL_CONVENTION_REFER||BHT.TRANSACTSET_PURPOSE_CODE||BHT.BATCH_CNTL_NO||BHT.TRANSACTSET_CREATE_DATE||BHT.TRANSACTSET_CREATE_TIME||BHT.TRANSACT_TYPE_CODE) AS XML_HDR_MD5,  
claimTrackingId.FILE_SOURCE, 
claimTrackingId.FILE_NAME  

FROM claimTrackingId 
     INNER JOIN GS ON claimTrackingId.XML_MD5 = GS.XML_MD5
     INNER JOIN ST ON GS.XML_MD5 = ST.XML_MD5 
     INNER JOIN BHT ON GS.XML_MD5 = BHT.XML_MD5
     LEFT JOIN Loop2300_CLM ON GS.XML_MD5 = Loop2300_CLM.XML_MD5
     LEFT JOIN Loop2300_DTP ON GS.XML_MD5 = Loop2300_DTP.XML_MD5
     WHERE Loop2300_DTP.XML_MD5 IS NOT NULL 
     
     
UNION ALL


select 

distinct 



GS.APP_SENDER_CODE,
GS.APP_RECIEVER_CODE,
GS.GRP_CONTROL_NO,
-------------
ST.TRANCACTSET_CNTL_NO,
ST.IMPL_CONVENTION_REFER,
-------------
BHT.TRANSACTSET_PURPOSE_CODE,
BHT.BATCH_CNTL_NO,

BHT.TRANSACTSET_CREATE_TIME,
BHT.TRANSACT_TYPE_CODE,
-------------



Loop2300_CLM.CLAIM_ID,

-------
Loop2400_LX.SL_SEQ_NUM,

------------
Loop2400_DTP.CLM_DATE_TYPE, 
Loop2400_DTP.CLM_DATE_FORMAT_QLFR, 
Loop2400_DTP.CLM_DATE,
--------------

------------
BHT.TRANSACTSET_CREATE_DATE,
----
  
claimTrackingId.claimTrackingId,  
claimTrackingId.XML_MD5,  
 MD5(GS.APP_SENDER_CODE||GS.APP_RECIEVER_CODE||GS.GRP_CONTROL_NO||ST.TRANCACTSET_CNTL_NO||ST.IMPL_CONVENTION_REFER||BHT.TRANSACTSET_PURPOSE_CODE||BHT.BATCH_CNTL_NO||BHT.TRANSACTSET_CREATE_DATE||BHT.TRANSACTSET_CREATE_TIME||BHT.TRANSACT_TYPE_CODE) AS XML_HDR_MD5,  
claimTrackingId.FILE_SOURCE, 
claimTrackingId.FILE_NAME  


FROM claimTrackingId 
     INNER JOIN GS ON claimTrackingId.XML_MD5 = GS.XML_MD5
     INNER JOIN ST ON GS.XML_MD5 = ST.XML_MD5 
     INNER JOIN BHT ON GS.XML_MD5 = BHT.XML_MD5
     LEFT JOIN Loop2300_CLM ON GS.XML_MD5 = Loop2300_CLM.XML_MD5
     LEFT JOIN Loop2400_LX  ON GS.XML_MD5 = Loop2400_LX.XML_MD5
     LEFT JOIN Loop2400_DTP ON Loop2400_LX.XML_MD5 = Loop2400_DTP.XML_MD5 AND Loop2400_LX.SL_SEQ_NUM = Loop2400_DTP.SL_SEQ_NUM
    where  Loop2400_DTP.clm_date_type is not null

;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC,  ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


UPDATE IDENTIFIER(:V_PROGRAM_LIST) SET  LAST_SUCCESSFUL_LOAD = :V_START_TIME WHERE  PROCESS_NAME = :V_PROCESS_NAME AND  SUB_PROCESS_NAME = :V_SUB_PROCESS_NAME; 
;




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC,  ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
