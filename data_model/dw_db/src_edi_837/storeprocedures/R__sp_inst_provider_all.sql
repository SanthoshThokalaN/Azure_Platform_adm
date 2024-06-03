USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_INST_PROVIDER_ALL"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''INST_PROVIDER_ALL'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_INST_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.INST_CLAIMS_RAW'';

V_INST_PROVIDER_ALL VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.INST_PROVIDER_ALL'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD INST_PROVIDER_ALL'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_INST_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );


INSERT INTO IDENTIFIER(:V_INST_PROVIDER_ALL)
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
  CLM_RENDERING_PRV_MAP , 
  CLM_ATTENDING_PRV_MAP , 
  CLM_REFERRING_PRV_MAP , 
  CLM_OPERATING_PHYS_MAP , 
  CLM_OTHER_OPERATING_PHYS_MAP , 
  SV_LX_NUMBER , 
  SV_RENDERING_PRV_MAP , 
  SV_REFERRING_PRV_MAP , 
  SV_OPERATING_PHYS_MAP , 
  SV_OTHER_OPERATING_PHYS_MAP ,
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
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_INST_PROVIDER_ALL) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
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
Loop2300_Loop2310D_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET ( NM1.value,''renderingPrvNameLast'' ): "$"::VARCHAR AS renderingPrvNameLast,
    XMLGET ( NM1.value,''renderingPrvNameFirst'' ): "$"::VARCHAR AS renderingPrvNameFirst,
    XMLGET ( NM1.value,''renderingPrvNameMiddle'' ): "$"::VARCHAR AS renderingPrvNameMiddle,
   XMLGET ( NM1.value,''renderingPrvNameSuffix'' ): "$"::VARCHAR AS renderingPrvNameSuffix,
    XMLGET ( NM1.value,''renderingPrvId'' ): "$"::VARCHAR AS renderingPrvId

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310D , LATERAL FLATTEN(to_array(GET(Loop2310D.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310D.value, ''@'') = ''Loop2310D''
    and GET( NM1.value, ''@'') in (''NM1'')

)


,

Loop2300_clm_rendering_prv_map as (

select Loop2300_Loop2310D_NM1.XML_MD5,
object_construct(
''renderingPrvNameLast'', nvl(Loop2300_Loop2310D_NM1.renderingPrvNameLast, ''''), 
''renderingPrvNameFirst'',nvl(Loop2300_Loop2310D_NM1.renderingPrvNameFirst, ''''), 
''renderingPrvNameMiddle'',nvl(Loop2300_Loop2310D_NM1.renderingPrvNameMiddle, ''''), 
''renderingPrvNameSuffix'',nvl(renderingPrvNameSuffix, ''''), 
''renderingPrvId'',nvl(Loop2300_Loop2310D_NM1.renderingPrvId, '''') 
) as clm_rendering_prv_map
from Loop2300_Loop2310D_NM1        

)


,


Loop2300_Loop2310A_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET ( NM1.value,''attendingPrvNameLast'' ): "$"::VARCHAR AS attendingPrvNameLast,
    XMLGET ( NM1.value,''attendingPrvNameFirst'' ): "$"::VARCHAR AS attendingPrvNameFirst,
    XMLGET ( NM1.value,''attendingPrvNameMiddle'' ): "$"::VARCHAR AS attendingPrvNameMiddle,
   XMLGET ( NM1.value,''attendingPrvNameSuffix'' ): "$"::VARCHAR AS attendingPrvNameSuffix,
    XMLGET ( NM1.value,''attendingPrvId'' ): "$"::VARCHAR AS attendingPrvId

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310A , LATERAL FLATTEN(to_array(GET(Loop2310A.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310A.value, ''@'') = ''Loop2310A''
    and GET( NM1.value, ''@'') in (''NM1'')

)



,




Loop2300_Loop2310A_PRV AS 
(
    select  
    XML_MD5, 
    XMLGET ( PRV.value,''attendingPrvSpecialtyTaxCode'' ): "$"::VARCHAR AS attendingPrvSpecialtyTaxCode
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310A , LATERAL FLATTEN(to_array(GET(Loop2310A.value,''$'')))  PRV
    where segment = ''Loop2300''
    and GET( Loop2310A.value, ''@'') = ''Loop2310A''
    and GET( PRV.value, ''@'') in (''PRV'')

)



,
Loop2300_Loop2310A as (

select Loop2300_Loop2310A_NM1.XML_MD5,
object_construct(
''prv_name_last'', nvl(attendingPrvNameLast, ''''), 
''prv_name_first'',nvl(attendingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(attendingPrvNameMiddle, ''''), 
''prv_name_suffix'',nvl(attendingPrvNameSuffix, ''''), 
''prv_id'',nvl(attendingPrvId, '''') ,
''prv_speciality_tax_code'',nvl(attendingPrvSpecialtyTaxCode, '''')
) as clm_attending_prv_map
from Loop2300_Loop2310A_NM1  LEFT JOIN Loop2300_Loop2310A_PRV ON Loop2300_Loop2310A_NM1.XML_MD5 = Loop2300_Loop2310A_PRV.XML_MD5        

)


,


Loop2300_Loop2310F_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET ( NM1.value,''referringPrvNameLast'' ): "$"::VARCHAR AS referringPrvNameLast,
    XMLGET ( NM1.value,''referringPrvNameFirst'' ): "$"::VARCHAR AS referringPrvNameFirst,
    XMLGET ( NM1.value,''referringPrvNameMiddle'' ): "$"::VARCHAR AS referringPrvNameMiddle,
      XMLGET ( NM1.value,''referringPrvNameSuffix'' ): "$"::VARCHAR AS referringPrvNameSuffix,
    XMLGET ( NM1.value,''referringPrvId'' ): "$"::VARCHAR AS referringPrvId

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310F , LATERAL FLATTEN(to_array(GET(Loop2310F.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310F.value, ''@'') = ''Loop2310F''
    and GET( NM1.value, ''@'') = ''NM1''

)

,

Loop2300_Loop2310F_REF_1G AS 
(
    select  
    XML_MD5, ''1G'' as refidqlfy,
    GET(REF_1G_A.VALUE, ''$'')::VARCHAR  as referringPrvSecondId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310F , LATERAL FLATTEN(to_array(GET(Loop2310F.value,''$'')))  REF_1G, LATERAL FLATTEN(to_array(GET(REF_1G.value,''$'')))  REF_1G_A
    where segment = ''Loop2300''
    and GET( Loop2310F.value, ''@'') = ''Loop2310F''
    and GET( REF_1G.value, ''@'') = ''REF_1G''

)
,

Loop2300_Loop2310F as (

select Loop2300_IDX.XML_MD5,
object_construct(

''prv_name_last'', nvl(referringPrvNameLast, ''''), 
''prv_name_first'',nvl(referringPrvNameFirst, ''''), 
''prv_name_middle'',nvl(referringPrvNameMiddle, ''''),
  ''prv_name_suffix'',nvl(referringPrvNameSuffix, ''''), 
''prv_id'',nvl(referringPrvId, ''''), 
''prv_ref_id_qlfr'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(referringPrvSecondId, '''')
) as clm_referring_prv_map
from Loop2300_IDX  LEFT JOIN Loop2300_Loop2310F_NM1    ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310F_NM1.XML_MD5
                   LEFT JOIN Loop2300_Loop2310F_REF_1G ON Loop2300_IDX.XML_MD5 = Loop2300_Loop2310F_REF_1G.XML_MD5
  WHERE (Loop2300_Loop2310F_NM1.XML_MD5 IS NOT NULL OR Loop2300_Loop2310F_REF_1G.XML_MD5 IS NOT NULL)
)



,


Loop2300_Loop2310B_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET ( NM1.value,''operatingPrvNameLast'' ): "$"::VARCHAR AS operatingPrvNameLast,
    XMLGET ( NM1.value,''operatingPrvNameFirst'' ): "$"::VARCHAR AS operatingPrvNameFirst,
    XMLGET ( NM1.value,''referringPrvNameMiddle'' ): "$"::VARCHAR AS operatingPrvNameMiddle,
   XMLGET ( NM1.value,''referringPrvNameSuffix'' ): "$"::VARCHAR AS operatingPrvNameSuffix,
    XMLGET ( NM1.value,''operatingPrvId'' ): "$"::VARCHAR AS operatingPrvId

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310B , LATERAL FLATTEN(to_array(GET(Loop2310B.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310B.value, ''@'') = ''Loop2310B''
    and GET( NM1.value, ''@'') in (''NM1'')

)

,

Loop2300_Loop2310B_NM1_MAP as (

select Loop2300_Loop2310B_NM1.XML_MD5,
object_construct(
''prv_name_last'', nvl(operatingPrvNameLast, ''''), 
''prv_name_first'',nvl(operatingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(operatingPrvNameMiddle, ''''), 
''prv_name_suffix'',nvl(operatingPrvNameSuffix, ''''), 
''prv_id'',nvl(operatingPrvId, '''') 
) as clm_operating_phys_map
from Loop2300_Loop2310B_NM1          

)


,
Loop2300_Loop2310C_NM1 AS 
(
    select  
    XML_MD5, 
    XMLGET ( NM1.value,''otherOperatingPrvNameLast'' ): "$"::VARCHAR AS otherOperatingPrvNameLast,
    XMLGET ( NM1.value,''otherOperatingPrvNameFirst'' ): "$"::VARCHAR AS otherOperatingPrvNameFirst,
    XMLGET ( NM1.value,''otherOperatingPrvNameMiddle'' ): "$"::VARCHAR AS otherOperatingPrvNameMiddle,
   XMLGET ( NM1.value,''otherOperatingPrvNameSuffix'' ): "$"::VARCHAR AS otherOperatingPrvNameSuffix,
    XMLGET ( NM1.value,''otherOperatingPrvId'' ): "$"::VARCHAR AS otherOperatingPrvId

    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310C , LATERAL FLATTEN(to_array(GET(Loop2310C.value,''$'')))  NM1
    where segment = ''Loop2300''
    and GET( Loop2310C.value, ''@'') = ''Loop2310C''
    and GET( NM1.value, ''@'') in (''NM1'')

)

,

Loop2300_Loop2310C_NM1_MAP as (

select Loop2300_Loop2310C_NM1.XML_MD5,
object_construct(
''prv_name_last'', nvl(otherOperatingPrvNameLast, ''''), 
''prv_name_first'',nvl(otherOperatingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(otherOperatingPrvNameMiddle, ''''), 
''prv_name_suffix'',nvl(otherOperatingPrvNameSuffix, ''''), 
''prv_id'',nvl(otherOperatingPrvId, '''') 
) as clm_other_operating_phys_map
from Loop2300_Loop2310C_NM1          

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

Loop2400_Loop2420C_NM1 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420C.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( NM1.value,''svRenderingPrvNameLast'' ): "$"::VARCHAR AS svRenderingPrvNameLast,
    XMLGET ( NM1.value,''svRenderingPrvNameFirst'' ): "$"::VARCHAR AS svRenderingPrvNameFirst,
    XMLGET ( NM1.value,''svRenderingPrvNameMiddle'' ): "$"::VARCHAR AS svRenderingPrvNameMiddle,
      XMLGET ( NM1.value,''svRenderingPrvNameSuffix'' ): "$"::VARCHAR AS svRenderingPrvNameSuffix,
    XMLGET ( NM1.value,''svRenderingPrvId'' ): "$"::VARCHAR AS svRenderingPrvId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420C , LATERAL FLATTEN(to_array(GET(Loop2420C.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420C.value, ''@'') = ''Loop2420C''
    and GET( NM1.value, ''@'') in (''NM1'')

)


,

Loop2400_Loop2420C_NM1_MAP as (

select Loop2400_LX.XML_MD5,
  Loop2400_Loop2420C_NM1.sl_seq_num,
object_construct(
''prv_name_last'', nvl(svRenderingPrvNameLast, ''''), 
''prv_name_first'',nvl(svRenderingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(svRenderingPrvNameMiddle, ''''), 
''prv_name_suffix'',nvl(svRenderingPrvNameSuffix, ''''), 
''prv_id'',nvl(svRenderingPrvId, '''') 
) as sv_rendering_prv_map
from Loop2400_LX left join  Loop2400_Loop2420C_NM1     on Loop2400_LX.XML_MD5=Loop2400_Loop2420C_NM1.XML_MD5

)



,

Loop2400_Loop2420D_NM1 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420D.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( NM1.value,''svReferringPrvNameLast'' ): "$"::VARCHAR AS svReferringPrvNameLast,
    XMLGET ( NM1.value,''svReferringPrvNameFirst'' ): "$"::VARCHAR AS svReferringPrvNameFirst,
    XMLGET ( NM1.value,''svReferringPrvNameMiddle'' ): "$"::VARCHAR AS svReferringPrvNameMiddle,
    XMLGET ( NM1.value,''svReferringPrvNameSuffix'' ): "$"::VARCHAR AS svReferringPrvNameSuffix,
    XMLGET ( NM1.value,''svReferringPrvId'' ): "$"::VARCHAR AS svReferringPrvId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420D , LATERAL FLATTEN(to_array(GET(Loop2420D.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420D.value, ''@'') = ''Loop2420D''
    and GET( NM1.value, ''@'') in (''NM1'')

)

,


Loop2400_Loop2420D_REF_1G_1 AS 
(
    select  
    XML_MD5, ''1G'' as refidqlfy,  XMLGET( Loop2420D.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num, 
    SUBSTR(GET(REF_1G_A.VALUE, ''@''),  1, LEN(GET(REF_1G_A.VALUE, ''@''))-1) AS key ,
    GET(REF_1G_A.VALUE, ''$'')::VARCHAR  as value,
    SUBSTR(GET(REF_1G_A.VALUE, ''@''),  -1, 1) AS grp
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420D , LATERAL FLATTEN(to_array(GET(Loop2420D.value,''$'')))  REF_1G
   , LATERAL FLATTEN(to_array(GET(REF_1G.value,''$'')))  REF_1G_A
    where segment = ''Loop2420D''
    and GET( Loop2420D.value, ''@'') = ''Loop2420D''
   and GET( REF_1G.value, ''@'') in (''REF_1G'')

)


,

Loop2400_Loop2420D_REF_1G (XML_MD5, REFIDQLFY, SL_SEQ_NUM, svReferringPrvSecondId) as 
(
select *  EXCLUDE (GRP)  from Loop2400_Loop2420D_REF_1G_1
PIVOT( MAX (VALUE) FOR KEY IN (''svReferringPrvSecondId'')) 
)

,
Loop_sv_referring_prv_map as (


select 
Loop2400_Loop2420D_NM1.XML_MD5,  
Loop2400_Loop2420D_NM1.SL_SEQ_NUM,


object_construct(

''prv_name_last'', nvl(svReferringPrvNameLast, ''''), 
''prv_name_first'',nvl(svReferringPrvNameFirst, ''''), 
''prv_name_middle'',nvl(svReferringPrvNameMiddle, ''''), 
  ''prv_name_suffix'',nvl(svReferringPrvNameSuffix, ''''), 
''prv_id'',nvl(svReferringPrvId, ''''), 
''prv_ref_id_qlfy_1'',nvl(refidqlfy, ''''),
''prv_second_id'',nvl(svReferringPrvSecondId, '''')
) as sv_referring_prv_map


from Loop2400_Loop2420D_NM1 
            
              LEFT JOIN Loop2400_Loop2420D_REF_1G     ON Loop2400_Loop2420D_NM1.XML_MD5 = Loop2400_Loop2420D_REF_1G.XML_MD5 AND Loop2400_Loop2420D_NM1.sl_seq_num = Loop2400_Loop2420D_REF_1G.sl_seq_num)
   

,

Loop2400_Loop2420A_NM1 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420A.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
  XMLGET ( NM1.value,''svOperatingPrvTypeQlfr'' ): "$"::VARCHAR AS svOperatingPrvTypeQlfr,
    XMLGET ( NM1.value,''svRenderingPrvNameLast'' ): "$"::VARCHAR AS svOperatingPrvNameLast,
    XMLGET ( NM1.value,''svRenderingPrvNameFirst'' ): "$"::VARCHAR AS svOperatingPrvNameFirst,
    XMLGET ( NM1.value,''svRenderingPrvNameMiddle'' ): "$"::VARCHAR AS svOperatingPrvNameMiddle,      
    XMLGET ( NM1.value,''svRenderingPrvId'' ): "$"::VARCHAR AS svOperatingPrvId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420A , LATERAL FLATTEN(to_array(GET(Loop2420A.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420A.value, ''@'') = ''Loop2420A''
    and GET( NM1.value, ''@'') in (''NM1'')

)


,

Loop2400_Loop2420A_NM1_MAP as (

select Loop2400_LX.XML_MD5,
  Loop2400_Loop2420A_NM1.sl_seq_num,
object_construct(
  ''prv_type_qlfr'', nvl(svOperatingPrvTypeQlfr, ''''), 
''prv_name_last'', nvl(svOperatingPrvNameLast, ''''), 
''prv_name_first'',nvl(svOperatingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(svOperatingPrvNameMiddle, ''''), 
''prv_id'',nvl(svOperatingPrvId, '''') 
) as sv_operating_phys_map
from Loop2400_LX left join  Loop2400_Loop2420A_NM1     on Loop2400_LX.XML_MD5=Loop2400_Loop2420A_NM1.XML_MD5

)



 
,

Loop2400_Loop2420B_NM1 AS 
(
    select  
    XML_MD5, XMLGET( Loop2420B.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( NM1.value,''svOtherOperatingPrvNameLast'' ): "$"::VARCHAR AS svOtherOperatingPrvNameLast,
    XMLGET ( NM1.value,''svOtherOperatingPrvNameFirst'' ): "$"::VARCHAR AS svOtherOperatingPrvNameFirst,
    XMLGET ( NM1.value,''svOtherOperatingPrvNameMiddle'' ): "$"::VARCHAR AS svOtherOperatingPrvNameMiddle,      
    XMLGET ( NM1.value,''svOtherOperatingPrvId'' ): "$"::VARCHAR AS svOtherOperatingPrvId
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2420B , LATERAL FLATTEN(to_array(GET(Loop2420B.value,''$'')))  NM1
    where segment = ''Loop2400''
    and GET( Loop2420B.value, ''@'') = ''Loop2420B''
    and GET( NM1.value, ''@'') in (''NM1'')

)


,

Loop2400_Loop2420B_NM1_MAP as (

select Loop2400_LX.XML_MD5,
  Loop2400_Loop2420B_NM1.sl_seq_num,
object_construct(

''prv_name_last'', nvl(svOtherOperatingPrvNameLast, ''''), 
''prv_name_first'',nvl(svOtherOperatingPrvNameFirst, ''''), 
''prv_name_middle'',nvl(svOtherOperatingPrvNameMiddle, ''''), 
''prv_id'',nvl(svOtherOperatingPrvId, '''') 
) as sv_other_operating_phys_map
from Loop2400_LX left join  Loop2400_Loop2420B_NM1     on Loop2400_LX.XML_MD5=Loop2400_Loop2420B_NM1.XML_MD5

)




select 
distinct 


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

Loop2300_CLM.claim_id,


coalesce(Loop2300_clm_rendering_prv_map.clm_rendering_prv_map,''{}''::variant) as clm_rendering_prv_map,


coalesce(Loop2300_Loop2310A.clm_attending_prv_map,''{}''::variant) as clm_attending_prv_map,

coalesce(Loop2300_Loop2310F.clm_referring_prv_map,''{}''::variant) as clm_referring_prv_map,



------

coalesce(Loop2300_Loop2310B_NM1_MAP.clm_operating_phys_map,''{}''::variant) as clm_operating_phys_map,


coalesce(Loop2300_Loop2310C_NM1_MAP.clm_other_operating_phys_map,''{}''::variant)   as clm_other_operating_phys_map,


COALESCE(Loop2400_LX.sl_seq_num,1) as sl_seq_num ,

-----
coalesce(Loop2400_Loop2420C_NM1_MAP.sv_rendering_prv_map,''{}''::variant) as sv_rendering_prv_map,

-------

-----

coalesce(Loop_sv_referring_prv_map.sv_referring_prv_map,''{}''::variant) as sv_referring_prv_map,

-----
coalesce(Loop2400_Loop2420A_NM1_MAP.sv_operating_phys_map,''{}''::variant) as sv_operating_phys_map,

coalesce(Loop2400_Loop2420B_NM1_MAP.sv_other_operating_phys_map,''{}''::variant) as sv_other_operating_phys_map,


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
     left join Loop2300_CLM ON GS.XML_MD5 = Loop2300_CLM.XML_MD5
        left join Loop2400_LX on GS.XML_MD5 = Loop2400_LX.XML_MD5 
        left join Loop2300_clm_rendering_prv_map on GS.XML_MD5 = Loop2300_clm_rendering_prv_map.XML_MD5
        left join Loop2300_Loop2310A on GS.XML_MD5 = Loop2300_Loop2310A.XML_MD5
        left join Loop2300_Loop2310F on GS.XML_MD5 = Loop2300_Loop2310F.XML_MD5
left join Loop2300_Loop2310B_NM1_MAP on GS.XML_MD5 = Loop2300_Loop2310B_NM1_MAP.XML_MD5
left join Loop2300_Loop2310C_NM1_MAP on GS.XML_MD5 = Loop2300_Loop2310C_NM1_MAP.XML_MD5
        left join Loop2400_Loop2420C_NM1_MAP on Loop2400_LX.XML_MD5 = Loop2400_Loop2420C_NM1_MAP.XML_MD5 and Loop2400_Loop2420C_NM1_MAP.sl_seq_num = Loop2400_LX.sl_seq_num
        left join Loop_sv_referring_prv_map on Loop2400_LX.XML_MD5 = Loop_sv_referring_prv_map.XML_MD5 and Loop_sv_referring_prv_map.sl_seq_num = Loop2400_LX.sl_seq_num
left join Loop2400_Loop2420A_NM1_MAP on Loop2400_LX.XML_MD5 = Loop2400_Loop2420A_NM1_MAP.XML_MD5 and Loop2400_Loop2420A_NM1_MAP.sl_seq_num = Loop2400_LX.sl_seq_num
left join Loop2400_Loop2420B_NM1_MAP on Loop2400_LX.XML_MD5 = Loop2400_Loop2420B_NM1_MAP.XML_MD5 and Loop2400_Loop2420B_NM1_MAP.sl_seq_num = Loop2400_LX.sl_seq_num



;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


UPDATE IDENTIFIER(:V_PROGRAM_LIST) SET  LAST_SUCCESSFUL_LOAD = :V_START_TIME WHERE  PROCESS_NAME = :V_PROCESS_NAME AND  SUB_PROCESS_NAME = :V_SUB_PROCESS_NAME; 
;




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
