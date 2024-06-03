USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_INST_PROVIDER"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''INST_PROVIDER'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_INST_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.INST_CLAIMS_RAW'';

V_INST_PROVIDER VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.INST_PROVIDER'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD INST_PROVIDER'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_INST_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );


INSERT INTO identifier(:V_INST_PROVIDER)
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
  PROVIDER_TAX_CODE , 
  PROVIDER_NAME , 
  PROVIDER_ID , 
  PROVIDER_ADDRESS_1 , 
  PROVIDER_ADDRESS_2 , 
  PROVIDER_CITY , 
  PROVIDER_STATE , 
  PROVIDER_POSTALCODE , 
  PROVIDER_TAX_ID , 
  PROVIDER_CONTACT_NAME , 
  PROVIDER_CONTACT_TYPE , 
  PROVIDER_CONTACT_NO , 
  PAYTO_ENT_TYPE , 
  PAYTO_ORGNIZATION_NAME , 
  PAYTO_ENT_ID ,
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
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_INST_PROVIDER) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
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


Loop2000A_PRV AS 
(
select 
XML_MD5, 
XMLGET ( PRV.value,''billingPrvTaxCode'' ): "$"::VARCHAR AS provider_tax_code
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) PRV
where segment = ''Loop2000A''
and GET(PRV.value, ''@'') = ''PRV''
)

,


Loop2010AA_NM1 

AS 
(
select 
XML_MD5, 

XMLGET ( NM1.value,''billingPrvNameLast'' ): "$"::VARCHAR AS provider_name,
XMLGET ( NM1.value,''billingPrvId'' ): "$"::VARCHAR AS provider_id
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) NM1
where segment = ''Loop2010AA''
and GET(NM1.value, ''@'') = ''NM1''
)
,


Loop2010AA_N3 AS 
(
select 
XML_MD5, 
XMLGET ( N3.value,''billingPrvAddress1'' ): "$"::VARCHAR AS provider_address_1,
XMLGET ( N3.value,''billingPrvAddress2'' ): "$"::VARCHAR AS provider_address_2
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) N3
where segment = ''Loop2010AA''
and GET(N3.value, ''@'') = ''N3''
)
,


Loop2010AA_N4 AS 
(
select 
XML_MD5, 
XMLGET ( N4.value,''billingPrvCity'' ): "$"::VARCHAR AS Provider_city,
XMLGET ( N4.value,''billingPrvState'' ): "$"::VARCHAR AS provider_state,
XMLGET ( N4.value,''billingPrvZip'' ): "$"::VARCHAR AS provider_postalcode
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) N4
where segment = ''Loop2010AA''
and GET(N4.value, ''@'') = ''N4''
)
,


Loop2010AA_REF_EI AS 
(
select 
XML_MD5, 
XMLGET ( REF_EI.value,''billingPrvTaxId'' ): "$"::VARCHAR AS provider_tax_id

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_EI
where segment = ''Loop2010AA''
and GET(REF_EI.value, ''@'') = ''REF_EI''
)

,


Loop2010AC_NM1 AS 
(
select 
XML_MD5, 
XMLGET ( NM1.value,''payToEntTypeQlfr'' ): "$"::Integer AS payto_ent_type,
XMLGET ( NM1.value,''payToOrganizationName'' ): "$"::VARCHAR AS payto_orgnization_name,
  XMLGET ( NM1.value,''payToEntId'' ): "$"::VARCHAR AS payto_ent_id
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) NM1
where segment = ''Loop2010AC''
and GET(NM1.value, ''@'') = ''NM1''
)


,

Loop2010AA_PER_1 AS 
(
select 
XML_MD5, 

 XMLGET( PER_A.value, ''billingPrvContactName'' ):"$"::VARCHAR  As provider_contact_name,
    SUBSTR(GET(PER_B.VALUE, ''@''),  1, LEN(GET(PER_B.VALUE, ''@''))-1) AS key,
    SUBSTR(GET(PER_B.VALUE, ''@''),  -1, 1) AS grp,
   GET(PER_B.VALUE, ''$'')::VARCHAR  as value
from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) PER, LATERAL FLATTEN(to_array(GET(PER.value,''$''))) PER_A  , LATERAL FLATTEN(to_array(GET(PER_A.value,''$''))) PER_B
where segment = ''Loop2010AA''
and GET(PER.value, ''@'') = ''PER''
and PER_B.INDEX in (1,2,3,4,5,6)
)

,

Loop2010AA_PER (XML_MD5, provider_contact_name, provider_contact_type, provider_contact_no)
as 
(
 
select *  exclude grp from Loop2010AA_PER_1
PIVOT( MAX (VALUE) FOR KEY IN (''billingPrvContactType'', ''billingPrvContactNumber''))  
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


PROVIDER_TAX_CODE,

PROVIDER_NAME,



PROVIDER_ID,
PROVIDER_ADDRESS_1,
PROVIDER_ADDRESS_2,
PROVIDER_CITY,
PROVIDER_STATE,
PROVIDER_POSTALCODE,

PROVIDER_TAX_ID,
PROVIDER_CONTACT_NAME,
PROVIDER_CONTACT_TYPE,
PROVIDER_CONTACT_NO,
payto_ent_type,
payto_orgnization_name,
payto_ent_id,
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

     LEFT JOIN Loop2000A_PRV ON GS.XML_MD5 = Loop2000A_PRV.XML_MD5
LEFT JOIN Loop2010AA_NM1 ON GS.XML_MD5 = Loop2010AA_NM1.XML_MD5 
LEFT JOIN Loop2010AA_N3  ON GS.XML_MD5 = Loop2010AA_N3.XML_MD5
LEFT JOIN Loop2010AA_N4  ON GS.XML_MD5 = Loop2010AA_N4.XML_MD5 
LEFT JOIN Loop2010AA_REF_EI ON GS.XML_MD5 = Loop2010AA_REF_EI.XML_MD5  
LEFT JOIN Loop2010AC_NM1  ON GS.XML_MD5 = Loop2010AC_NM1.XML_MD5

LEFT JOIN Loop2010AA_PER  ON GS.XML_MD5 = Loop2010AA_PER.XML_MD5
;

V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   

   
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'',  :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


UPDATE IDENTIFIER(:V_PROGRAM_LIST) SET  LAST_SUCCESSFUL_LOAD = :V_START_TIME WHERE  PROCESS_NAME = :V_PROCESS_NAME AND  SUB_PROCESS_NAME = :V_SUB_PROCESS_NAME; 
;




EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'' , :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
