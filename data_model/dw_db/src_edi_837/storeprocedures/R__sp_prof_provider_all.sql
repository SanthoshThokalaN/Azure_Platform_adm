USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_PROF_PROVIDER_ALL"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''PROF_PROVIDER_ALL'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_PROF_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.PROF_CLAIMS_RAW'';

V_PROF_PROVIDER_ALL VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.PROF_PROVIDER_ALL'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD PROF_PROVIDER_ALL'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_PROF_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );

INSERT INTO IDENTIFIER(:V_PROF_PROVIDER_ALL)
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
  CLM_RENDERING_PRV_MAP , 
  CLM_SUPERVISING_PRV_MAP , 
  CLM_REFERRING_PRV_MAP , 
  SL_SEQ_NUM , 
  SV_RENDERING_PRV_MAP , 
  SV_SUPERVISING_PRV_MAP , 
  SV_REFERRING_PRV_MAP , 
  SV_ORDERING_PRV_MAP ,
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
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_PROF_PROVIDER_ALL) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
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

Loop2300_IDX AS 
(
select 
XML_MD5, IDX
from segments
where segment = ''Loop2300''
 
)
,


Loop2300_Loop2310B_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET ( NM1.value,''renderingPrvTypeQlfr'' ): "$"::VARCHAR AS renderingPrvTypeQlfr,
    XMLGET ( NM1.value,''renderingPrvNameLast'' ): "$"::VARCHAR AS renderingPrvNameLast,
    XMLGET ( NM1.value,''renderingPrvNameFirst'' ): "$"::VARCHAR AS renderingPrvNameFirst,
    XMLGET ( NM1.value,''renderingPrvNameMiddle'' ): "$"::VARCHAR AS renderingPrvNameMiddle,
    XMLGET ( NM1.value,''renderingPrvNameSuffix'' ): "$"::VARCHAR AS renderingPrvNameSuffix,
    XMLGET ( NM1.value,''renderingPrvId'' ): "$"::VARCHAR AS renderingPrvId

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310B , LATERAL FLATTEN(to_array(GET(Loop2310B.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310B.value, ''@'') = ''Loop2310B''
    and GET( NM1.value, ''@'') in (''NM1'')

)


,

Loop2300_Loop2310B_PRV AS 
(
    select  
    XML_MD5, 
    XMLGET ( PRV.value,''renderingPrvSpecialtyIdQlfr'' ): "$"::VARCHAR AS renderingPrvSpecialtyIdQlfr,
    XMLGET ( PRV.value,''renderingPrvSpecialtyTaxCode'' ): "$"::VARCHAR AS renderingPrvSpecialtyTaxCode
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310B , LATERAL FLATTEN(to_array(GET(Loop2310B.value,''$'')))  PRV
    where segment = ''Loop2300''
    and GET( Loop2310B.value, ''@'') = ''Loop2310B''
    and GET( PRV.value, ''@'') in (''PRV'')

)
,

Loop2300_Loop2310B_REF_G2 AS 
(
    select  
    XML_MD5, ''G2'' as refidqlfy,
    GET(REF_G2_A.VALUE, ''$'')::VARCHAR  as renderingPrvSecondId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310B , LATERAL FLATTEN(to_array(GET(Loop2310B.value,''$'')))  REF_G2, LATERAL FLATTEN(to_array(GET(REF_G2.value,''$'')))  REF_G2_A
    where segment = ''Loop2300''
    and GET( Loop2310B.value, ''@'') = ''Loop2310B''
    and GET( REF_G2.value, ''@'') in (''REF_G2'')

)
,

Loop2300 as (

select Loop2300_IDX.XML_MD5,  

renderingPrvTypeQlfr, renderingPrvNameLast, renderingPrvNameFirst, renderingPrvNameMiddle,renderingPrvNameSuffix, renderingPrvId, 

renderingPrvSpecialtyIdQlfr, renderingPrvSpecialtyTaxCode,

refidqlfy, renderingPrvSecondId

from       Loop2300_IDX        LEFT JOIN Loop2300_Loop2310B_NM1 ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310B_NM1.XML_MD5               
                           LEFT JOIN Loop2300_Loop2310B_PRV ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310B_PRV.XML_MD5 
                           LEFT JOIN Loop2300_Loop2310B_REF_G2 ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310B_REF_G2.XML_MD5 
where (Loop2300_Loop2310B_NM1.XML_MD5 is not null or Loop2300_Loop2310B_PRV.XML_MD5 is not null or Loop2300_Loop2310B_REF_G2.XML_MD5 is not null )
)

,

Loop2320_Seq AS 
(
select 
XML_MD5, IDX
from segments
where segment = ''Loop2320''
)



,

Loop2320_Loop2330D_NM1 AS 
(
    select   
    XML_MD5, 
    segments.IDX, 
    XMLGET ( NM1.value,''otherPayerRenderingPrvTypeQlfr'' ): "$"::VARCHAR AS otherPayerRenderingPrvTypeQlfr
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2330D , LATERAL FLATTEN(to_array(GET(Loop2330D.value,''$'')))  NM1
    where segment = ''Loop2320''
    and GET( Loop2330D.value, ''@'') = ''Loop2330D''
    and GET( NM1.value, ''@'') in (''NM1'')

)

,

Loop2320 as (

select Loop2320_Seq.XML_MD5,  
Loop2320_Loop2330D_NM1.otherPayerRenderingPrvTypeQlfr
from       Loop2320_Seq  LEFT JOIN Loop2320_Loop2330D_NM1 ON Loop2320_Seq.XML_MD5 = Loop2320_Loop2330D_NM1.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_Loop2330D_NM1.IDX   
where Loop2320_Loop2330D_NM1.XML_MD5 is not null
)
,

Loop2300_Loop2320 as (

select Loop2300.XML_MD5,
object_construct(
''prv_type_qlfr'', nvl(renderingPrvTypeQlfr, ''''), 
''prv_name_last'',nvl(renderingPrvNameLast, ''''), 
''prv_name_first'',nvl(renderingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(renderingPrvNameMiddle, ''''), 
''prv_name_suffix'', nvl(renderingPrvNameSuffix, ''''),
''prv_id'',nvl(renderingPrvId, '''') ,
''prv_speciality_id_qlfr'',nvl(renderingPrvSpecialtyIdQlfr, ''''), 
''prv_speciality_tax_code'',nvl(renderingPrvSpecialtyTaxCode, ''''),
''prv_ref_id_qlfy'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(renderingPrvSecondId, ''''), 
''other_payer_ren_prv_type'', nvl(otherPayerRenderingPrvTypeQlfr, '''')
) as clm_rendering_prv_map
from Loop2300  LEFT JOIN Loop2320 ON Loop2300.XML_MD5 = Loop2320.XML_MD5        

)

,

Loop2300_Loop2310D_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET ( NM1.value,''supervisingPrvTypeQlfr'' ): "$"::VARCHAR AS supervisingPrvTypeQlfr,
    XMLGET ( NM1.value,''supervisingPrvNameLast'' ): "$"::VARCHAR AS supervisingPrvNameLast,
    XMLGET ( NM1.value,''supervisingPrvNameFirst'' ): "$"::VARCHAR AS supervisingPrvNameFirst,
    XMLGET ( NM1.value,''supervisingPrvNameMiddle'' ): "$"::VARCHAR AS supervisingPrvNameMiddle,
    XMLGET ( NM1.value,''supervisingPrvNameSuffix'' ): "$"::VARCHAR AS supervisingPrvNameSuffix,
    XMLGET ( NM1.value,''supervisingPrvId'' ): "$"::VARCHAR AS supervisingPrvId

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310D , LATERAL FLATTEN(to_array(GET(Loop2310D.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310D.value, ''@'') = ''Loop2310D''
    and GET( NM1.value, ''@'') = ''NM1''

)

,

Loop2300_Loop2310D_REF_G2 AS 
(
    select  
    XML_MD5, ''G2'' as refidqlfy,
    GET(REF_G2_A.VALUE, ''$'')::VARCHAR  as supervisingPrvSecondId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310D , LATERAL FLATTEN(to_array(GET(Loop2310D.value,''$'')))  REF_G2, LATERAL FLATTEN(to_array(GET(REF_G2.value,''$'')))  REF_G2_A
    where segment = ''Loop2300''
    and GET( Loop2310D.value, ''@'') = ''Loop2310D''
    and GET( REF_G2.value, ''@'') = ''REF_G2''

)
,

Loop2300_Loop2310D as (

select Loop2300_IDX.XML_MD5,
object_construct(
''prv_type_qlfr'', nvl(supervisingPrvTypeQlfr, ''''), 
''prv_name_last'', nvl(supervisingPrvNameLast, ''''), 
''prv_name_first'',nvl(supervisingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(supervisingPrvNameMiddle, ''''),
 ''prv_name_suffix'',nvl( supervisingPrvNameSuffix, ''''),
''prv_id'',nvl(supervisingPrvId, ''''), 
''prv_ref_id_qlfr'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(supervisingPrvSecondId, '''')
) as clm_supervising_prv_map
from Loop2300_IDX  LEFT JOIN Loop2300_Loop2310D_NM1    ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310D_NM1.XML_MD5
               LEFT JOIN Loop2300_Loop2310D_REF_G2 ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310D_REF_G2.XML_MD5
  WHERE (Loop2300_Loop2310D_NM1.XML_MD5 IS NOT NULL OR Loop2300_Loop2310D_REF_G2.XML_MD5 IS NOT NULL)
)
,


Loop2300_Loop2310A_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET ( NM1.value,''referringPrvTypeQlfr'' ): "$"::VARCHAR AS referringPrvTypeQlfr,
    XMLGET ( NM1.value,''referringPrvNameLast'' ): "$"::VARCHAR AS referringPrvNameLast,
    XMLGET ( NM1.value,''referringPrvNameFirst'' ): "$"::VARCHAR AS referringPrvNameFirst,
    XMLGET ( NM1.value,''referringPrvNameMiddle'' ): "$"::VARCHAR AS referringPrvNameMiddle,
    XMLGET ( NM1.value,''referringPrvNameSuffix'' ): "$"::VARCHAR AS referringPrvNameSuffix,
    XMLGET ( NM1.value,''referringPrvId'' ): "$"::VARCHAR AS referringPrvId

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310A , LATERAL FLATTEN(to_array(GET(Loop2310A.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310A.value, ''@'') = ''Loop2310A''
    and GET( NM1.value, ''@'') = ''NM1''

)

,

Loop2300_Loop2310A_REF_G2 AS 
(
    select  
    XML_MD5, ''G2'' as refidqlfy,
    GET(REF_G2_A.VALUE, ''$'')::VARCHAR  as referringPrvSecondId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310A , LATERAL FLATTEN(to_array(GET(Loop2310A.value,''$'')))  REF_G2, LATERAL FLATTEN(to_array(GET(REF_G2.value,''$'')))  REF_G2_A
    where segment = ''Loop2300''
    and GET( Loop2310A.value, ''@'') = ''Loop2310A''
    and GET( REF_G2.value, ''@'') = ''REF_G2''

)
,

Loop2300_Loop2310A as (

select Loop2300_IDX.XML_MD5,
object_construct(
''prv_type_qlfr'', nvl(referringPrvTypeQlfr, ''''), 
''prv_name_last'', nvl(referringPrvNameLast, ''''), 
''prv_name_first'',nvl(referringPrvNameFirst, ''''), 
''prv_name_middle'',nvl(referringPrvNameMiddle, ''''), 
  ''prv_name_suffix'',nvl(referringPrvNameSuffix, ''''),
''prv_id'',nvl(referringPrvId, ''''), 
''prv_ref_id_qlfy'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(referringPrvSecondId, '''')
) as clm_referring_prv_map
from Loop2300_IDX  LEFT JOIN Loop2300_Loop2310A_NM1    ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310A_NM1.XML_MD5
                   LEFT JOIN Loop2300_Loop2310A_REF_G2 ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310A_REF_G2.XML_MD5
  WHERE (Loop2300_Loop2310A_NM1.XML_MD5 IS NOT NULL OR Loop2300_Loop2310A_REF_G2.XML_MD5 IS NOT NULL)
)

,




Loop2400_Loop2420A_NM1 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420A.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( NM1.value,''svRenderingPrvTypeQlfr'' ): "$"::VARCHAR AS svRenderingPrvTypeQlfr,
    XMLGET ( NM1.value,''svRenderingPrvNameLast'' ): "$"::VARCHAR AS svRenderingPrvNameLast,
    XMLGET ( NM1.value,''svRenderingPrvNameFirst'' ): "$"::VARCHAR AS svRenderingPrvNameFirst,
    XMLGET ( NM1.value,''svRenderingPrvNameMiddle'' ): "$"::VARCHAR AS svRenderingPrvNameMiddle,
    XMLGET ( NM1.value,''svRenderingPrvNameSuffix'' ): "$"::VARCHAR AS svRenderingPrvNameSuffix,
    XMLGET ( NM1.value,''svRenderingPrvId'' ): "$"::VARCHAR AS svRenderingPrvId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420A , LATERAL FLATTEN(to_array(GET(Loop2420A.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420A.value, ''@'') = ''Loop2420A''
    and GET( NM1.value, ''@'') in (''NM1'')

)


,

Loop2400_Loop2420A_PRV AS 
(
    select  
    XML_MD5, XMLGET( Loop2420A.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( PRV.value,''svRenderingPrvSpecialtyIdQlfr'' ): "$"::VARCHAR AS svRenderingPrvSpecialtyIdQlfr,
    XMLGET ( PRV.value,''svRenderingPrvSpecialtyTaxCode'' ): "$"::VARCHAR AS svRenderingPrvSpecialtyTaxCode
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420A , LATERAL FLATTEN(to_array(GET(Loop2420A.value,''$'')))  PRV
    where segment = ''Loop2400''
    and GET( Loop2420A.value, ''@'') = ''Loop2420A''
    and GET( PRV.value, ''@'') in (''PRV'')

)


,

Loop2400_Loop2420A_REF_G2_1 AS 
(
    select  
    XML_MD5, ''G2'' as refidqlfy,  XMLGET( Loop2420A.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    SUBSTR(GET(REF_G2_A.VALUE, ''@''),  1, LEN(GET(REF_G2_A.VALUE, ''@''))-1) AS key ,
    GET(REF_G2_A.VALUE, ''$'')::VARCHAR  as value,
    SUBSTR(GET(REF_G2_A.VALUE, ''@''),  -1, 1) AS grp
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420A , LATERAL FLATTEN(to_array(GET(Loop2420A.value,''$'')))  REF_G2
   , LATERAL FLATTEN(to_array(GET(REF_G2.value,''$'')))  REF_G2_A
    where segment = ''Loop2400''
    and GET( Loop2420A.value, ''@'') = ''Loop2420A''
   and GET( REF_G2.value, ''@'') in (''REF_G2'')

)


,

Loop2400_Loop2420A_REF_G2 (XML_MD5, REFIDQLFY, SL_SEQ_NUM, svRenderingPrvSecondId, svRenderingPrvOtherPayerId) as 
(
select *  EXCLUDE (GRP)  from Loop2400_Loop2420A_REF_G2_1
PIVOT( MAX (VALUE) FOR KEY IN (''svRenderingPrvSecondId'',''svRenderingPrvOtherPayerId'')) 
),

Loop_sv_rendering_prv_map as (
select 
Loop2400_Loop2420A_NM1.XML_MD5,  
Loop2400_Loop2420A_NM1.SL_SEQ_NUM,


object_construct(
''prv_type_qlfr'', nvl(svRenderingPrvTypeQlfr, ''''), 
''prv_name_last'', nvl(svRenderingPrvNameLast, ''''), 
''prv_name_first'',nvl(svRenderingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(svRenderingPrvNameMiddle, ''''), 
   ''prv_name_suffix'',nvl(svRenderingPrvNameSuffix, ''''),
''prv_id'',nvl(svRenderingPrvId, ''''), 
''prv_speciality_id_qlfr'',nvl(svRenderingPrvSpecialtyIdQlfr, ''''), 
''prv_speciality_tax_code'',nvl(svRenderingPrvSpecialtyTaxCode, ''''), 
''prv_ref_id_qlfy_1'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(svRenderingPrvSecondId, ''''),
''prv_other_payer_id'',nvl(svRenderingPrvOtherPayerId, '''')
) as sv_rendering_prv_map


from Loop2400_Loop2420A_NM1 
              LEFT JOIN Loop2400_Loop2420A_PRV ON Loop2400_Loop2420A_NM1.XML_MD5 = Loop2400_Loop2420A_PRV.XML_MD5 AND Loop2400_Loop2420A_NM1.sl_seq_num = Loop2400_Loop2420A_PRV.sl_seq_num
               LEFT JOIN Loop2400_Loop2420A_REF_G2     ON Loop2400_Loop2420A_NM1.XML_MD5 = Loop2400_Loop2420A_REF_G2.XML_MD5 AND Loop2400_Loop2420A_NM1.sl_seq_num = Loop2400_Loop2420A_REF_G2.sl_seq_num)
              

,


Loop2400_Loop2420D_NM1 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420D.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( NM1.value,''svSupervisingPrvTypeQlfr'' ): "$"::VARCHAR AS svSupervisingPrvTypeQlfr,
    XMLGET ( NM1.value,''svSupervisingPrvNameLast'' ): "$"::VARCHAR AS svSupervisingPrvNameLast,
    XMLGET ( NM1.value,''svSupervisingPrvNameFirst'' ): "$"::VARCHAR AS svSupervisingPrvNameFirst,
    XMLGET ( NM1.value,''svSupervisingPrvNameMiddle'' ): "$"::VARCHAR AS svSupervisingPrvNameMiddle,
    XMLGET ( NM1.value,''svSupervisingPrvNameSuffix'' ): "$"::VARCHAR AS svSupervisingPrvNameSuffix,
    XMLGET ( NM1.value,''svSupervisingPrvId'' ): "$"::VARCHAR AS svSupervisingPrvId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420D , LATERAL FLATTEN(to_array(GET(Loop2420D.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420D.value, ''@'') = ''Loop2420D''
    and GET( NM1.value, ''@'') in (''NM1'')

)

,


Loop2400_Loop2420D_REF_G2_1 AS 
(
    select  
    XML_MD5, ''G2'' as refidqlfy,  XMLGET( Loop2420D.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    SUBSTR(GET(REF_G2_A.VALUE, ''@''),  1, LEN(GET(REF_G2_A.VALUE, ''@''))-1) AS key ,
    GET(REF_G2_A.VALUE, ''$'')::VARCHAR  as value,
    SUBSTR(GET(REF_G2_A.VALUE, ''@''),  -1, 1) AS grp
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420D , LATERAL FLATTEN(to_array(GET(Loop2420D.value,''$'')))  REF_G2
   , LATERAL FLATTEN(to_array(GET(REF_G2.value,''$'')))  REF_G2_A
    where segment = ''Loop2400''
    and GET( Loop2420D.value, ''@'') = ''Loop2420D''
   and GET( REF_G2.value, ''@'') in (''REF_G2'')

)


,

Loop2400_Loop2420D_REF_G2 (XML_MD5, REFIDQLFY, SL_SEQ_NUM, svSupervisingPrvSecondId, svSupervisingPrvOtherPayerId) as 
(
select *  EXCLUDE (GRP)  from Loop2400_Loop2420D_REF_G2_1
PIVOT( MAX (VALUE) FOR KEY IN (''svSupervisingPrvSecondId'',''svSupervisingPrvOtherPayerId'')) 
)

,
Loop_sv_supervising_prv_map as (

select 
Loop2400_Loop2420D_NM1.XML_MD5,  
Loop2400_Loop2420D_NM1.SL_SEQ_NUM,


object_construct(
''prv_type_qlfr'', nvl(svSupervisingPrvTypeQlfr, ''''), 
''prv_name_last'', nvl(svSupervisingPrvNameLast, ''''), 
''prv_name_first'',nvl(svSupervisingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(svSupervisingPrvNameMiddle, ''''), 
   ''prv_name_suffix'',nvl(svSupervisingPrvNameSuffix, ''''),
''prv_id'',nvl(svSupervisingPrvId, ''''), 
''prv_ref_id_qlfy'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(svSupervisingPrvSecondId, ''''),
''prv_other_payer_id'',nvl(svSupervisingPrvOtherPayerId, '''')
) as sv_supervising_prv_map


from Loop2400_Loop2420D_NM1 
            
              LEFT JOIN Loop2400_Loop2420D_REF_G2     ON Loop2400_Loop2420D_NM1.XML_MD5 = Loop2400_Loop2420D_REF_G2.XML_MD5 AND Loop2400_Loop2420D_NM1.sl_seq_num = Loop2400_Loop2420D_REF_G2.sl_seq_num)
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
              
-------------------------


Loop2400_Loop2420F_NM1 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420F.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( NM1.value,''svReferringPrvTypeQlfr'' ): "$"::VARCHAR AS svReferringPrvTypeQlfr,
    XMLGET ( NM1.value,''svReferringPrvNameLast'' ): "$"::VARCHAR AS svReferringPrvNameLast,
    XMLGET ( NM1.value,''svReferringPrvNameFirst'' ): "$"::VARCHAR AS svReferringPrvNameFirst,
    XMLGET ( NM1.value,''svReferringPrvNameMiddle'' ): "$"::VARCHAR AS svReferringPrvNameMiddle,
    XMLGET ( NM1.value,''svReferringPrvNameSuffix'' ): "$"::VARCHAR AS svReferringPrvNameSuffix,
    XMLGET ( NM1.value,''svReferringPrvId'' ): "$"::VARCHAR AS svReferringPrvId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420F , LATERAL FLATTEN(to_array(GET(Loop2420F.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420F.value, ''@'') = ''Loop2420F''
    and GET( NM1.value, ''@'') in (''NM1'')

)

,


Loop2400_Loop2420F_REF_G2_1 AS 
(
    select  
    XML_MD5, ''G2'' as refidqlfy,  XMLGET( Loop2420F.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    SUBSTR(GET(REF_G2_A.VALUE, ''@''),  1, LEN(GET(REF_G2_A.VALUE, ''@''))-1) AS key ,
    GET(REF_G2_A.VALUE, ''$'')::VARCHAR  as value,
    SUBSTR(GET(REF_G2_A.VALUE, ''@''),  -1, 1) AS grp
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420F , LATERAL FLATTEN(to_array(GET(Loop2420F.value,''$'')))  REF_G2
   , LATERAL FLATTEN(to_array(GET(REF_G2.value,''$'')))  REF_G2_A
    where segment = ''Loop2420F''
    and GET( Loop2420F.value, ''@'') = ''Loop2420F''
   and GET( REF_G2.value, ''@'') in (''REF_G2'')

)


,

Loop2400_Loop2420F_REF_G2 (XML_MD5, REFIDQLFY, SL_SEQ_NUM, svReferringPrvSecondId, svReferringPrvOtherPayerPrimaryId) as 
(
select *  EXCLUDE (GRP)  from Loop2400_Loop2420F_REF_G2_1
PIVOT( MAX (VALUE) FOR KEY IN (''svReferringPrvSecondId'',''svReferringPrvOtherPayerPrimaryId'')) 
)

,
Loop_sv_referring_prv_map as (


select 
Loop2400_Loop2420F_NM1.XML_MD5,  
Loop2400_Loop2420F_NM1.SL_SEQ_NUM,


object_construct(
''prv_type_qlfr'', nvl(svReferringPrvTypeQlfr, ''''), 
''prv_name_last'', nvl(svReferringPrvNameLast, ''''), 
''prv_name_first'',nvl(svReferringPrvNameFirst, ''''), 
''prv_name_middle'',nvl(svReferringPrvNameMiddle, ''''), 
  ''prv_name_suffix'',nvl(svReferringPrvNameSuffix, ''''), 
''prv_id'',nvl(svReferringPrvId, ''''), 
''prv_ref_id_qlfy'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(svReferringPrvSecondId, ''''),
''prv_other_payer_id'',nvl(svReferringPrvOtherPayerPrimaryId, '''')
) as sv_referring_prv_map


from Loop2400_Loop2420F_NM1 
            
              LEFT JOIN Loop2400_Loop2420F_REF_G2     ON Loop2400_Loop2420F_NM1.XML_MD5 = Loop2400_Loop2420F_REF_G2.XML_MD5 AND Loop2400_Loop2420F_NM1.sl_seq_num = Loop2400_Loop2420F_REF_G2.sl_seq_num)
   

,

Loop2400_Loop2420E_NM1 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420E.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( NM1.value,''svOrderingPrvTypeQlfr'' ): "$"::VARCHAR AS svOrderingPrvTypeQlfr,
    XMLGET ( NM1.value,''svOrderingPrvNameLast'' ): "$"::VARCHAR AS svOrderingPrvNameLast,
    XMLGET ( NM1.value,''svOrderingPrvNameFirst'' ): "$"::VARCHAR AS svOrderingPrvNameFirst,
    XMLGET ( NM1.value,''svOrderingPrvNameMiddle'' ): "$"::VARCHAR AS svOrderingPrvNameMiddle,
    XMLGET ( NM1.value,''svOrderingPrvNameSuffix'' ): "$"::VARCHAR AS svOrderingPrvNameSuffix,
    XMLGET ( NM1.value,''svOrderingPrvId'' ): "$"::VARCHAR AS svOrderingPrvId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420E , LATERAL FLATTEN(to_array(GET(Loop2420E.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420E.value, ''@'') = ''Loop2420E''
    and GET( NM1.value, ''@'') in (''NM1'')

)
 
,

Loop2400_Loop2420E_N3 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420E.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( N3.value,''svOrderingPrvAddress1'' ): "$"::VARCHAR AS svOrderingPrvAddress1,
    XMLGET ( N3.value,''svOrderingPrvAddress2'' ): "$"::VARCHAR AS svOrderingPrvAddress2

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420E , LATERAL FLATTEN(to_array(GET(Loop2420E.value,''$'')))  N3
    where segment = ''Loop2400''
    and GET( Loop2420E.value, ''@'') = ''Loop2420E''
    and GET( N3.value, ''@'') in (''N3'')

)

,

Loop2400_Loop2420E_N4 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420E.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( N4.value,''svOrderingPrvCity'' ): "$"::VARCHAR AS svOrderingPrvCity,
    XMLGET ( N4.value,''svOrderingPrvState'' ): "$"::VARCHAR AS svOrderingPrvState,
    XMLGET ( N4.value,''svOrderingPrvZip'' ): "$"::VARCHAR AS svOrderingPrvZip
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420E , LATERAL FLATTEN(to_array(GET(Loop2420E.value,''$'')))  N4
    where segment = ''Loop2400''
    and GET( Loop2420E.value, ''@'') = ''Loop2420E''
    and GET( N4.value, ''@'') in (''N4'')

)

,

Loop2400_Loop2420E_REF_G2_1 AS 
(
    select  
    XML_MD5, ''G2'' as refidqlfy,  XMLGET( Loop2420E.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    SUBSTR(GET(REF_G2_A.VALUE, ''@''),  1, LEN(GET(REF_G2_A.VALUE, ''@''))-1) AS key ,
    GET(REF_G2_A.VALUE, ''$'')::VARCHAR  as value,
    SUBSTR(GET(REF_G2_A.VALUE, ''@''),  -1, 1) AS grp
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420E , LATERAL FLATTEN(to_array(GET(Loop2420E.value,''$'')))  REF_G2
   , LATERAL FLATTEN(to_array(GET(REF_G2.value,''$'')))  REF_G2_A
    where segment = ''Loop2420E''
    and GET( Loop2420E.value, ''@'') = ''Loop2420E''
   and GET( REF_G2.value, ''@'') in (''REF_G2'')

)

 
,

Loop2400_Loop2420E_REF_G2 (XML_MD5, REFIDQLFY, SL_SEQ_NUM, svOrderingPrvSecondId, svOrderingPrvOtherPayerId) as 
(
select *  EXCLUDE (GRP)  from Loop2400_Loop2420E_REF_G2_1
PIVOT( MAX (VALUE) FOR KEY IN (''svOrderingPrvSecondId'',''svOrderingPrvOtherPayerId'')) 
)

,

Loop2400_Loop2420E_PER AS 
(
    select  
    XML_MD5, XMLGET( Loop2420E.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( PER.value,''svOrderingPrvContactType1'' ): "$"::VARCHAR AS svOrderingPrvContactType1,
    XMLGET ( PER.value,''svOrderingPrvContactNumber1'' ): "$"::VARCHAR AS svOrderingPrvContactNumber1,
    XMLGET ( PER.value,''svOrderingPrvContactType2'' ): "$"::VARCHAR AS svOrderingPrvContactType2,
    XMLGET ( PER.value,''svOrderingPrvContactNumber2'' ): "$"::VARCHAR AS svOrderingPrvContactNumber2,
    XMLGET ( PER.value,''svOrderingPrvContactType3'' ): "$"::VARCHAR AS svOrderingPrvContactType3,
    XMLGET ( PER.value,''svOrderingPrvContactNumber3'' ): "$"::VARCHAR AS svOrderingPrvContactNumber3
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420E , LATERAL FLATTEN(to_array(GET(Loop2420E.value,''$'')))  PER
    where segment = ''Loop2400''
    and GET( Loop2420E.value, ''@'') = ''Loop2420E''
    and GET( PER.value, ''@'') in (''PER'')

)
,

Loop_sv_ordering_prv_map as (


select 
Loop2400_Loop2420E_NM1.XML_MD5,  
Loop2400_Loop2420E_NM1.SL_SEQ_NUM,


object_construct(
''prv_type_qlfr'', nvl(svOrderingPrvTypeQlfr, ''''), 
''prv_name_last'', nvl(svOrderingPrvNameLast, ''''), 
''prv_name_first'',nvl(svOrderingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(svOrderingPrvNameMiddle, ''''), 
''prv_name_suffix'',nvl(svOrderingPrvNameSuffix, ''''), 
''prv_id'',nvl(svOrderingPrvId, ''''), 
''prv_ref_id_qlfy_1'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(svOrderingPrvSecondId, ''''),
''prv_otherpayer_id'',nvl(svOrderingPrvOtherPayerId, ''''),
''prv_address_1'',nvl(svOrderingPrvAddress1, ''''),
  ''prv_address_2'',nvl(svOrderingPrvAddress2, ''''),
  ''provider_city'',nvl(svOrderingPrvCity, ''''),
  ''prv_contact_number_2'',nvl(svOrderingPrvContactNumber2, ''''),
  ''prv_contact_number_1'',nvl(svOrderingPrvContactNumber1, ''''),
  ''provider_state'',nvl(svOrderingPrvState, ''''),
  ''prv_contact_number_3'',nvl(svOrderingPrvContactNumber3, ''''),
  ''prv_contact_type_2'',nvl(svOrderingPrvContactType2, ''''),
 ''prv_contact_type_3'',nvl(svOrderingPrvContactType3, ''''),
  ''provider_zip'',nvl(svOrderingPrvZip, ''''),
  ''prv_contact_type_1'',nvl(svOrderingPrvContactType1, '''')
) as sv_ordering_prv_map


from Loop2400_Loop2420E_NM1 
            
              LEFT JOIN Loop2400_Loop2420E_REF_G2     ON Loop2400_Loop2420E_NM1.XML_MD5 = Loop2400_Loop2420E_REF_G2.XML_MD5 AND Loop2400_Loop2420E_NM1.sl_seq_num = Loop2400_Loop2420E_REF_G2.sl_seq_num
              LEFT JOIN Loop2400_Loop2420E_PER  ON Loop2400_Loop2420E_NM1.XML_MD5 = Loop2400_Loop2420E_PER.XML_MD5 AND Loop2400_Loop2420E_NM1.sl_seq_num = Loop2400_Loop2420E_PER.sl_seq_num
 LEFT JOIN Loop2400_Loop2420E_N3     ON Loop2400_Loop2420E_NM1.XML_MD5 = Loop2400_Loop2420E_N3.XML_MD5 AND Loop2400_Loop2420E_NM1.sl_seq_num = Loop2400_Loop2420E_N3.sl_seq_num
 LEFT JOIN Loop2400_Loop2420E_N4     ON Loop2400_Loop2420E_NM1.XML_MD5 = Loop2400_Loop2420E_N4.XML_MD5 AND Loop2400_Loop2420E_NM1.sl_seq_num = Loop2400_Loop2420E_N4.sl_seq_num
)
       

       


  select 



---------------
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
Loop2000B_HL.provider_hl_no,
Loop2000B_HL.subscriber_hl_no,
Loop2000B_HL.payer_hl_no,
----------
Loop2300_CLM.claim_id,


Loop2300_Loop2320.clm_rendering_prv_map,


Loop2300_Loop2310D.clm_supervising_prv_map,

Loop2300_Loop2310A.clm_referring_prv_map,



------


COALESCE(Loop2400_LX.sl_seq_num,1) as sl_seq_num ,

-----
Loop_sv_rendering_prv_map.sv_rendering_prv_map,

-------
Loop_sv_supervising_prv_map.sv_supervising_prv_map,


-----

Loop_sv_referring_prv_map.sv_referring_prv_map,

-----
Loop_sv_ordering_prv_map.sv_ordering_prv_map,


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
     LEFT JOIN Loop2000B_HL ON GS.XML_MD5 = Loop2000B_HL.XML_MD5
     left join Loop2300_CLM ON GS.XML_MD5 = Loop2300_CLM.XML_MD5
        left join Loop2400_LX on GS.XML_MD5 = Loop2400_LX.XML_MD5 
        left join LOOP2300_LOOP2320 on GS.XML_MD5 = LOOP2300_LOOP2320.XML_MD5
        left join Loop2300_Loop2310D on GS.XML_MD5 = Loop2300_Loop2310D.XML_MD5
        left join Loop2300_Loop2310A on GS.XML_MD5 = Loop2300_Loop2310A.XML_MD5
        left join Loop_sv_rendering_prv_map on GS.XML_MD5 = Loop_sv_rendering_prv_map.XML_MD5
        left join Loop_sv_supervising_prv_map on GS.XML_MD5 = Loop_sv_supervising_prv_map.XML_MD5
left join Loop_sv_referring_prv_map on GS.XML_MD5 = Loop_sv_referring_prv_map.XML_MD5
left join Loop_sv_ordering_prv_map on GS.XML_MD5 = Loop_sv_ordering_prv_map.XML_MD5
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
