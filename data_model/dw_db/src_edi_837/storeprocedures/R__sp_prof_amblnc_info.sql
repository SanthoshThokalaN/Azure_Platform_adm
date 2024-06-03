USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_PROF_AMBLNC_INFO"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''PROF_AMBLNC_INFO'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_PROF_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.PROF_CLAIMS_RAW'';

V_PROF_AMBLNC_INFO VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.PROF_AMBLNC_INFO'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD PROF_AMBLNC_INFO'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_PROF_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );

INSERT INTO identifier(:V_PROF_AMBLNC_INFO)
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
  PROVIDER_HL_NO , 
  SUBSCRIBER_HL_NO , 
  PAYER_HL_NO , 
  CLAIM_ID , 
  CLM_AMBLNC_WEIGHT_MEASURE_UNIT , 
  CLM_AMBLNC_PAT_WEIGHT , 
  CLM_AMBLNC_TRANSPORT_REASON_CD , 
  CLM_AMBLNC_DISTANCE_MEASURE_UNIT , 
  CLM_AMBLNC_TRANSPORT_DISTANCE , 
  CLM_AMBLNC_ROUNDTRIP_DESC , 
  CLM_AMBLNC_STRETCHER_DESC , 
  CLM_AMBLNC_PICKUP_ADDR_1 , 
  CLM_AMBLNC_PICKUP_ADDR_2 , 
  CLM_AMBLNC_PICKUP_CITY , 
  CLM_AMBLNC_PICKUP_STATE , 
  CLM_AMBLNC_PICKUP_ZIP , 
  CLM_AMBLNC_DROPOFF_LOCATION , 
  CLM_AMBLNC_DROPOFF_ADDR_1 , 
  CLM_AMBLNC_DROPOFF_ADDR_2 , 
  CLM_AMBLNC_DROPOFF_CITY , 
  CLM_AMBLNC_DROPOFF_STATE , 
  CLM_AMBLNC_DROPOFF_ZIP , 
  CLM_AMBLNC_CERTIFICATION , 
  SV_LX_NUMBER , 
  SV_AMBLNC_WEIGHT_MEASURE_UNIT , 
  SV_AMBLNC_PAT_WEIGHT , 
  SV_AMBLNC_TRANSPORT_REASON_CD , 
  SV_AMBLNC_DISTANCE_MEASURE_UNIT , 
  SV_AMBLNC_TRANSPORT_DISTANCE ,
  SV_AMBLNC_ROUNDTRIP_DESC , 
  SV_AMBLNC_STRETCHER_DESC , 
  SV_AMBLNC_CERTIFICATION , 
  SV_AMBLNC_PAT_COUNT , 
  SV_AMBLNC_PICKUP_ADDR_1 , 
  SV_AMBLNC_PICKUP_ADDR_2 , 
  SV_AMBLNC_PICKUP_CITY , 
  SV_AMBLNC_PICKUP_STATE , 
  SV_AMBLNC_PICKUP_ZIP , 
  SV_AMBLNC_DROPOFF_LOCATION , 
  SV_AMBLNC_DROPOFF_ADDR_1 , 
  SV_AMBLNC_DROPOFF_ADDR_2 , 
  SV_AMBLNC_DROPOFF_CITY , 
  SV_AMBLNC_DROPOFF_STATE , 
  SV_AMBLNC_DROPOFF_ZIP ,
  TRANSACTSET_CREATE_DATE ,
  CLAIM_TRACKING_ID ,
  XML_MD5 ,
  XML_HDR_MD5 ,
  FILE_SOURCE ,
  FILE_NAME
)   
     
WITH 


RAW AS (
SELECT * FROM IDENTIFIER(:V_PROF_CLAIMS_RAW) R WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_PROF_AMBLNC_INFO) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
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


Loop2000B_HL AS 
(
select 
XML_MD5, 
XMLGET ( HL.value,''prvHlNumber'' ): "$"::VARCHAR AS provider_hl_no,
XMLGET ( HL.value,''subscriberHlNumber'' ): "$"::VARCHAR AS subscriber_hl_no,
XMLGET ( HL.value,''subscriberHlNumber'' ): "$"::VARCHAR AS payer_hl_no
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HL
where segment = ''Loop2000B''
and GET(HL.value, ''@'') = ''HL''
)


,

Loop2300_CLM AS 
(
select 
XML_MD5, 
XMLGET ( CLM.value,''patControlNumber'' ): "$"::VARCHAR AS claim_id
from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) CLM
where segment = ''Loop2300''
and GET(CLM.value, ''@'') = ''CLM''
)



,

Loop2300_CR1 AS 
(
select 
XML_MD5, 
  XMLGET ( CR1.value,''amblncWeightMeasureUnit'' ): "$"::VARCHAR AS clm_amblnc_weight_measure_unit,
  XMLGET ( CR1.value,''amblncPatWeight'' ): "$"::VARCHAR AS clm_amblnc_pat_weight,
  XMLGET ( CR1.value,''amblncTransportReasonCode'' ): "$"::VARCHAR AS clm_amblnc_transport_reason_cd,
  XMLGET ( CR1.value,''amblncDistanceMeasureUnit'' ): "$"::VARCHAR AS clm_amblnc_distance_measure_unit,
  XMLGET ( CR1.value,''amblncTransportDistance'' ): "$"::VARCHAR AS clm_amblnc_transport_distance,
  XMLGET ( CR1.value,''amblncRoundtripDesc'' ): "$"::VARCHAR AS clm_amblnc_roundtrip_desc,
  XMLGET ( CR1.value,''amblncStretcherDesc'' ): "$"::VARCHAR AS clm_amblnc_stretcher_desc
from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) CR1
where segment = ''Loop2300''
and GET(CR1.value, ''@'') = ''CR1''
)

,

Loop2300_CRC_07_1 AS 
(
select 
XML_MD5, XMLGET( CRC_07_A.value, ''amblncResponseCode'' ):"$"::VARCHAR  As amblncResponseCode, 
  
   GET(CRC_07_B.VALUE, ''$'')::VARCHAR  as amblncConditionIndicator,
        SUBSTR(GET(CRC_07_A.VALUE, ''@''),  1, LEN(GET(CRC_07_A.VALUE, ''@''))) AS grp

from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) CRC_07, LATERAL FLATTEN(to_array(GET(CRC_07.value,''$''))) CRC_07_A, LATERAL FLATTEN(to_array(GET(CRC_07_A.value,''$''))) CRC_07_B
where segment = ''Loop2300''
and GET(CRC_07.value, ''@'') = ''CRC_07''
and CRC_07_B.INDEX in (1,2,3,4,5)
)
, 

Loop2300_CRC_07_2 as (

select XML_MD5, AMBLNCRESPONSECODE,  ARRAY_AGG(AMBLNCCONDITIONINDICATOR) as amblncConditionIndicator from Loop2300_CRC_07_1
group by XML_MD5, AMBLNCRESPONSECODE, GRP
)
,

Loop2300_CRC_07 as (
select XML_MD5,  parse_json(concat(''['', listagg(concat(''{'',replace(replace(substr(array_construct(OBJECT_CONSTRUCT(''clm_amblnc_cert_condition_indicator'', AMBLNCRESPONSECODE) , OBJECT_CONSTRUCT(''clm_amblnc_condition_codes'', amblncConditionIndicator))::varchar, 2, 
                       length(array_construct(OBJECT_CONSTRUCT(''clm_amblnc_cert_condition_indicator'', AMBLNCRESPONSECODE) , OBJECT_CONSTRUCT(''clm_amblnc_condition_codes'', amblncConditionIndicator))::varchar)-2), ''}'', ''''), ''{'', ''''), ''}'')
                         , '',''), '']'')) as clm_amblnc_certification               
from Loop2300_CRC_07_2
group by XML_MD5
)

,

Loop2300_Loop2310E_N3 AS 
(
    select  
    XML_MD5, 
    segments.IDX, 
    XMLGET ( N3.value,''amblncPickUpAddress1'' ): "$"::VARCHAR AS clm_amblnc_pickup_addr_1,
    XMLGET ( N3.value,''amblncPickUpAddress2'' ): "$"::VARCHAR AS clm_amblnc_pickup_addr_2
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310E, LATERAL FLATTEN(to_array(GET(Loop2310E.value,''$'')))  N3
    where segment = ''Loop2300''
    and GET( Loop2310E.value, ''@'') = ''Loop2310E''
    and GET( N3.value, ''@'') = ''N3''

)

,

Loop2300_Loop2310E_N4 AS 
(
    select  
    XML_MD5, 
    segments.IDX, 
    XMLGET ( N4.value,''amblncPickUpCity'' ): "$"::VARCHAR AS clm_amblnc_pickup_city,
    XMLGET ( N4.value,''amblncPickUpState'' ): "$"::VARCHAR AS clm_amblnc_pickup_state,
      XMLGET ( N4.value,''amblncPickUpZip'' ): "$"::VARCHAR AS clm_amblnc_pickup_zip
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310E, LATERAL FLATTEN(to_array(GET(Loop2310E.value,''$'')))  N4
    where segment = ''Loop2300''
    and GET( Loop2310E.value, ''@'') = ''Loop2310E''
    and GET( N4.value, ''@'') = ''N4''

)


,

Loop2300_Loop2310F_NM1 AS 
(
    select  
    XML_MD5, 
    segments.IDX, 
    XMLGET ( NM1.value,''amblncDropOffLocation'' ): "$"::VARCHAR AS clm_amblnc_dropoff_location
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310F, LATERAL FLATTEN(to_array(GET(Loop2310F.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310F.value, ''@'') = ''Loop2310F''
    and GET( NM1.value, ''@'') = ''NM1''

)
,

Loop2300_Loop2310F_N3 AS 
(
    select  
    XML_MD5, 
    segments.IDX, 
    XMLGET ( N3.value,''amblncDropOffAddress1'' ): "$"::VARCHAR AS clm_amblnc_dropoff_addr_1,
    XMLGET ( N3.value,''amblncDropOffAddress2'' ): "$"::VARCHAR AS clm_amblnc_dropoff_addr_2
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310F, LATERAL FLATTEN(to_array(GET(Loop2310F.value,''$'')))  N3
    where segment = ''Loop2300''
    and GET( Loop2310F.value, ''@'') = ''Loop2310F''
    and GET( N3.value, ''@'') = ''N3''

)

,

Loop2300_Loop2310F_N4 AS 
(
    select  
    XML_MD5, 
    segments.IDX, 
    XMLGET ( N4.value,''amblncDropOffCity'' ): "$"::VARCHAR AS clm_amblnc_dropoff_city,
    XMLGET ( N4.value,''amblncDropOffState'' ): "$"::VARCHAR AS clm_amblnc_dropoff_state,
    XMLGET ( N4.value,''amblncDropOffZip'' ): "$"::VARCHAR AS clm_amblnc_dropoff_zip
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310F, LATERAL FLATTEN(to_array(GET(Loop2310F.value,''$'')))  N4
    where segment = ''Loop2300''
    and GET( Loop2310F.value, ''@'') = ''Loop2310F''
    and GET( N4.value, ''@'') = ''N4''

)

,

Loop2400_LX AS 
(
select 
XML_MD5, 
XMLGET ( LX.value,''svSeqNumber'' ): "$"::VARCHAR AS sl_seq_num
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) LX
where segment = ''Loop2400''
and GET(LX.value, ''@'') = ''LX''
)

,

Loop2400_CR1 AS 
(
select 
XML_MD5,  XML, 
XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET ( CR1.value,''svAmblncWeightMeasureUnit'' ): "$"::VARCHAR AS sv_amblnc_weight_measure_unit,
XMLGET ( CR1.value,''svAmblncPatWeight'' ): "$"::VARCHAR AS sv_amblnc_pat_weight,
XMLGET ( CR1.value,''svAmblncTransportReasonCode'' ): "$"::VARCHAR AS sv_amblnc_transport_reason_cd,
XMLGET ( CR1.value,''svAmblncDistanceMeasureUnit'' ): "$"::VARCHAR AS sv_amblnc_distance_measure_unit,
XMLGET ( CR1.value,''svAmblncTransportDistance'' ): "$"::VARCHAR AS sv_amblnc_transport_distance,
XMLGET ( CR1.value,''svAmblncRoundTripDesc'' ): "$"::VARCHAR AS sv_amblnc_roundtrip_desc,
XMLGET ( CR1.value,''svAmblncStretcherDesc'' ): "$"::VARCHAR AS sv_amblnc_stretcher_desc
from segments S , LATERAL FLATTEN(to_array(GET(xml,''$''))) CR1 
where segment = ''Loop2400''
and GET(CR1.value, ''@'') = ''CR1''
)

,

Loop2400_QTY_PT AS 
(
select 
XML_MD5,  
XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET ( QTY_PT.value,''svAmblncPatCount'' ): "$"::VARCHAR AS sv_amblnc_pat_count

from segments S , LATERAL FLATTEN(to_array(GET(xml,''$''))) QTY_PT 
where segment = ''Loop2400''
and GET(QTY_PT.value, ''@'') = ''QTY_PT''
)


,




Loop2400_CRC_07_1 AS 
(
select 
XML_MD5, 
  XMLGET( CRC_07.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
  XMLGET( CRC_07_A.value, ''svAmblncResponseCode'' ):"$"::VARCHAR  As svAmblncResponseCode, 
  
   GET(CRC_07_B.VALUE, ''$'')::VARCHAR  as svAmblncConditionIndicator,
        SUBSTR(GET(CRC_07_A.VALUE, ''@''),  1, LEN(GET(CRC_07_A.VALUE, ''@''))) AS grp

from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) CRC_07, LATERAL FLATTEN(to_array(GET(CRC_07.value,''$''))) CRC_07_A, LATERAL FLATTEN(to_array(GET(CRC_07_A.value,''$''))) CRC_07_B
where segment = ''Loop2400''
and GET(CRC_07.value, ''@'') = ''CRC_07''
and CRC_07_B.INDEX in (1,2,3,4,5)
)




, 

Loop2400_CRC_07_2 as (

select XML_MD5, sl_seq_num, SVAMBLNCRESPONSECODE,  ARRAY_AGG(SVAMBLNCCONDITIONINDICATOR) as svamblncConditionIndicator from Loop2400_CRC_07_1
group by XML_MD5,sl_seq_num,  SVAMBLNCRESPONSECODE, GRP
)


,

Loop2400_CRC_07 as (
select XML_MD5,  sl_seq_num, parse_json(concat(''['', listagg(concat(''{'',replace(replace(substr(array_construct(OBJECT_CONSTRUCT(''sv_amblnc_cert_condition_indicator'', SVAMBLNCRESPONSECODE) , OBJECT_CONSTRUCT(''sv_amblnc_condition_codes'', svamblncConditionIndicator))::varchar, 2, 
                       length(array_construct(OBJECT_CONSTRUCT(''sv_amblnc_cert_condition_indicator'', svAMBLNCRESPONSECODE) , OBJECT_CONSTRUCT(''sv_amblnc_condition_codes'', svamblncConditionIndicator))::varchar)-2), ''}'', ''''), ''{'', ''''), ''}'')
                         , '',''), '']'')) as sv_amblnc_certification               
from Loop2400_CRC_07_2
group by XML_MD5, sl_seq_num
)

,


Loop2400_Loop2420G_N3 AS 
(
    select  
    XML_MD5, 
    XMLGET( Loop2420G.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    XMLGET ( N3.value,''svAmblncPickUpAddress1'' ): "$"::VARCHAR AS sv_amblnc_pickup_addr_1,
    XMLGET ( N3.value,''svAmblncPickUpAddress2'' ): "$"::VARCHAR AS sv_amblnc_pickup_addr_2
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420G, LATERAL FLATTEN(to_array(GET(Loop2420G.value,''$'')))  N3
    where segment = ''Loop2400''
    and GET( Loop2420G.value, ''@'') = ''Loop2420G''
    and GET( N3.value, ''@'') = ''N3''

)

,

Loop2400_Loop2420G_N4 AS 
(
    select  
    XML_MD5, 
    XMLGET( Loop2420G.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( N4.value,''svAmblncPickUpCity'' ): "$"::VARCHAR AS sv_amblnc_pickup_city,
    XMLGET ( N4.value,''svAmblncPickUpState'' ): "$"::VARCHAR AS sv_amblnc_pickup_state,
      XMLGET ( N4.value,''svAmblncPickUpZip'' ): "$"::VARCHAR AS sv_amblnc_pickup_zip
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420G, LATERAL FLATTEN(to_array(GET(Loop2420G.value,''$'')))  N4
    where segment = ''Loop2400''
    and GET( Loop2420G.value, ''@'') = ''Loop2420G''
    and GET( N4.value, ''@'') = ''N4''

)


,

Loop2400_Loop2420H_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET( Loop2420H.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    XMLGET ( NM1.value,''svAmblncDropOffLocation'' ): "$"::VARCHAR AS sv_amblnc_dropoff_location
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420H, LATERAL FLATTEN(to_array(GET(Loop2420H.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420H.value, ''@'') = ''Loop2420H''
    and GET( NM1.value, ''@'') = ''NM1''

)


,

Loop2400_Loop2420H_N3 AS 
(
    select  
    XML_MD5, 
    XMLGET( Loop2420H.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    XMLGET ( N3.value,''svAmblncDropOffAddress1'' ): "$"::VARCHAR AS sv_amblnc_dropoff_addr_1,
    XMLGET ( N3.value,''svAmblncDropOffAddress2'' ): "$"::VARCHAR AS sv_amblnc_dropoff_addr_2
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420H, LATERAL FLATTEN(to_array(GET(Loop2420H.value,''$'')))  N3
    where segment = ''Loop2400''
    and GET( Loop2420H.value, ''@'') = ''Loop2420H''
    and GET( N3.value, ''@'') = ''N3''

)



,

Loop2400_Loop2420H_N4 AS 
(
    select  
    XML_MD5,  
    XMLGET( Loop2420H.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    XMLGET ( N4.value,''svAmblncDropOffCity'' ): "$"::VARCHAR AS sv_amblnc_dropoff_city,
    XMLGET ( N4.value,''svAmblncDropOffState'' ): "$"::VARCHAR AS sv_amblnc_dropoff_state,
    XMLGET ( N4.value,''svAmblncDropOffZip'' ): "$"::VARCHAR AS  sv_amblnc_dropoff_zip
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420H, LATERAL FLATTEN(to_array(GET(Loop2420H.value,''$'')))  N4
    where segment = ''Loop2400''
    and GET( Loop2420H.value, ''@'') = ''Loop2420H''
    and GET( N4.value, ''@'') = ''N4''

)

  

 
SELECT
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
Loop2000B_HL.PROVIDER_HL_NO,
Loop2000B_HL.SUBSCRIBER_HL_NO,
Loop2000B_HL.PAYER_HL_NO,
--------------

Loop2300_CLM.CLAIM_ID,

-------------


  Loop2300_CR1.CLM_AMBLNC_WEIGHT_MEASURE_UNIT , 
  Loop2300_CR1.CLM_AMBLNC_PAT_WEIGHT , 
  Loop2300_CR1.CLM_AMBLNC_TRANSPORT_REASON_CD , 
  Loop2300_CR1.CLM_AMBLNC_DISTANCE_MEASURE_UNIT , 
  Loop2300_CR1.CLM_AMBLNC_TRANSPORT_DISTANCE , 
  Loop2300_CR1.CLM_AMBLNC_ROUNDTRIP_DESC , 
  Loop2300_CR1.CLM_AMBLNC_STRETCHER_DESC , 
  Loop2300_Loop2310E_N3.CLM_AMBLNC_PICKUP_ADDR_1 , 
  Loop2300_Loop2310E_N3.CLM_AMBLNC_PICKUP_ADDR_2 , 
  Loop2300_Loop2310E_N4.CLM_AMBLNC_PICKUP_CITY , 
  Loop2300_Loop2310E_N4.CLM_AMBLNC_PICKUP_STATE , 
  Loop2300_Loop2310E_N4.CLM_AMBLNC_PICKUP_ZIP , 
  Loop2300_Loop2310F_NM1.CLM_AMBLNC_DROPOFF_LOCATION , 
  Loop2300_Loop2310F_N3.CLM_AMBLNC_DROPOFF_ADDR_1 , 
  Loop2300_Loop2310F_N3.CLM_AMBLNC_DROPOFF_ADDR_2 , 
  Loop2300_Loop2310F_N4.CLM_AMBLNC_DROPOFF_CITY , 
  Loop2300_Loop2310F_N4.CLM_AMBLNC_DROPOFF_STATE , 
  Loop2300_Loop2310F_N4.CLM_AMBLNC_DROPOFF_ZIP, 
  Loop2300_CRC_07.CLM_AMBLNC_CERTIFICATION , 
  Loop2400_LX.sl_seq_num AS SV_LX_NUMBER, 
  Loop2400_CR1.SV_AMBLNC_WEIGHT_MEASURE_UNIT , 
  Loop2400_CR1.SV_AMBLNC_PAT_WEIGHT , 
  Loop2400_CR1.SV_AMBLNC_TRANSPORT_REASON_CD , 
  Loop2400_CR1.SV_AMBLNC_DISTANCE_MEASURE_UNIT , 
  Loop2400_CR1.SV_AMBLNC_TRANSPORT_DISTANCE , 
  Loop2400_CR1.SV_AMBLNC_ROUNDTRIP_DESC , 
  Loop2400_CR1.SV_AMBLNC_STRETCHER_DESC , 
  Loop2400_CRC_07.SV_AMBLNC_CERTIFICATION , 
  Loop2400_QTY_PT.SV_AMBLNC_PAT_COUNT , 
  Loop2400_Loop2420G_N3.SV_AMBLNC_PICKUP_ADDR_1 , 
  Loop2400_Loop2420G_N3.SV_AMBLNC_PICKUP_ADDR_2 , 
  Loop2400_Loop2420G_N4.SV_AMBLNC_PICKUP_CITY , 
  Loop2400_Loop2420G_N4.SV_AMBLNC_PICKUP_STATE , 
  Loop2400_Loop2420G_N4.SV_AMBLNC_PICKUP_ZIP , 
  Loop2400_Loop2420H_NM1.SV_AMBLNC_DROPOFF_LOCATION , 
  Loop2400_Loop2420H_N3.SV_AMBLNC_DROPOFF_ADDR_1 , 
  Loop2400_Loop2420H_N3.SV_AMBLNC_DROPOFF_ADDR_2 , 
  Loop2400_Loop2420H_N4.SV_AMBLNC_DROPOFF_CITY , 
  Loop2400_Loop2420H_N4.SV_AMBLNC_DROPOFF_STATE , 
  Loop2400_Loop2420H_N4.SV_AMBLNC_DROPOFF_ZIP, 
  
  BHT.TRANSACTSET_CREATE_DATE,
 
  claimTrackingId.claimTrackingId,  
  claimTrackingId.XML_MD5,  
  MD5(GS.APP_SENDER_CODE||GS.APP_RECIEVER_CODE||GS.GRP_CONTROL_NO||ST.TRANCACTSET_CNTL_NO||ST.IMPL_CONVENTION_REFER||BHT.TRANSACTSET_PURPOSE_CODE||BHT.BATCH_CNTL_NO||BHT.TRANSACTSET_CREATE_DATE||BHT.TRANSACTSET_CREATE_TIME||BHT.TRANSACT_TYPE_CODE) AS XML_HDR_MD5,  
  claimTrackingId.FILE_SOURCE, 
  claimTrackingId.FILE_NAME  


FROM claimTrackingId 
     INNER JOIN GS ON claimTrackingId.XML_MD5 = GS.XML_MD5
     INNER JOIN ST ON GS.XML_MD5 = ST.XML_MD5 
     INNER JOIN BHT ON GS.XML_MD5 = BHT.XML_MD5
     LEFT JOIN Loop2000B_HL ON GS.XML_MD5 = Loop2000B_HL.XML_MD5
     LEFT JOIN Loop2300_CLM ON GS.XML_MD5 = Loop2300_CLM.XML_MD5
     LEFT JOIN Loop2300_CR1 ON GS.XML_MD5 = Loop2300_CR1.XML_MD5    
     LEFT JOIN Loop2300_Loop2310E_N3 ON GS.XML_MD5 = Loop2300_Loop2310E_N3.XML_MD5
     LEFT JOIN Loop2300_Loop2310E_N4 ON GS.XML_MD5 = Loop2300_Loop2310E_N4.XML_MD5
     LEFT JOIN Loop2300_Loop2310F_NM1 ON GS.XML_MD5 = Loop2300_Loop2310F_NM1.XML_MD5
     LEFT JOIN Loop2300_Loop2310F_N3 ON GS.XML_MD5 = Loop2300_Loop2310F_N3.XML_MD5
     LEFT JOIN Loop2300_Loop2310F_N4 ON GS.XML_MD5 = Loop2300_Loop2310F_N4.XML_MD5
     LEFT JOIN Loop2300_CRC_07 ON GS.XML_MD5 = Loop2300_CRC_07.XML_MD5
         
     LEFT JOIN Loop2400_LX  ON GS.XML_MD5 = Loop2400_LX.XML_MD5
     LEFT JOIN Loop2400_CR1 ON Loop2400_LX.XML_MD5 = Loop2400_CR1.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_CR1.sl_seq_num
     LEFT JOIN Loop2400_CRC_07 ON Loop2400_LX.XML_MD5 = Loop2400_CRC_07.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_CRC_07.sl_seq_num
     LEFT JOIN Loop2400_QTY_PT ON Loop2400_LX.XML_MD5 = Loop2400_QTY_PT.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_QTY_PT.sl_seq_num
     LEFT JOIN Loop2400_Loop2420G_N3 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2420G_N3.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_Loop2420G_N3.sl_seq_num
     LEFT JOIN Loop2400_Loop2420G_N4 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2420G_N4.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_Loop2420G_N4.sl_seq_num
     LEFT JOIN Loop2400_Loop2420H_NM1 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2420H_NM1.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_Loop2420H_NM1.sl_seq_num
     LEFT JOIN Loop2400_Loop2420H_N3 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2420H_N3.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_Loop2420H_N3.sl_seq_num
     LEFT JOIN Loop2400_Loop2420H_N4 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2420H_N4.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_Loop2420H_N4.sl_seq_num

;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


UPDATE IDENTIFIER(:V_PROGRAM_LIST) SET  LAST_SUCCESSFUL_LOAD = :V_START_TIME WHERE  PROCESS_NAME = :V_PROCESS_NAME AND  SUB_PROCESS_NAME = :V_SUB_PROCESS_NAME; 
;




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';

