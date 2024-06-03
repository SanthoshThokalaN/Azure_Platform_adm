USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_INST_SUBSCRIBER"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''INST_SUBSCRIBER'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_INST_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.INST_CLAIMS_RAW'';

V_INST_SUBSCRIBER VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.INST_SUBSCRIBER'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD INST_SUBSCRIBER'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_INST_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );

INSERT INTO IDENTIFIER(:V_INST_SUBSCRIBER)
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
  PAYER_RESPONSIBILITY_CODE , 
  SUBSCRIBER_RELATIONSHIP_CODE , 
  SUBSCRIBER_POLICY_NUMBER , 
  SUBSCRIBER_PLAN_NAME , 
  SUBSCRIBER_INSURANCE_TYPE , 
  SUBSCRIBER_COB_CODE , 
  YES_NO_RESPONSE_CODE , 
  SUBSCRIBER_EMPLOYEMENT_STATUS , 
  CLAIM_TYPE , 
  SUBSCRIBER_TYPE , 
  SUBSCRIBER_NAME_LAST , 
  SUBSCRIBER_NAME_FIRST , 
  SUBSCRIBER_NAME_MIDDLE , 
  NAME_PREFIX , 
  NAME_SUFFIX , 
  SUBSCRIBER_ID_QUALIFIER , 
  SUBSCRIBER_ID , 
  SUBSCRIBER_ADDRESS1 , 
  SUBSCRIBER_ADDRESS2 , 
  SUBSCRIBER_CITY , 
  SUBSCRIBER_STATE , 
  SUBSCRIBER_POSTALCODE , 
  SUBSCRIBER_DATEOFBIRTH , 
  SUBSCRIBER_GENDERCODE , 
  OTHER_PAYER_RESPONSIBILITY_CODE , 
  OTHER_SUBSCRIBER_RELATIONSHIP_CODE , 
  OTHER_SUBSCRIBER_POLICY_NUMBER , 
  OTHER_SUBSCRIBER_PLAN_NAME , 
  OTHER_CLM_FILLING_IND_CD , 
  OTHER_SUBSCRIBER_TYPE , 
  OTHER_SUBSCRIBER_NAME_LAST , 
  OTHER_SUBSCRIBER_NAME_FIRST , 
  OTHER_SUBSCRIBER_NAME_MIDDLE , 
  OTHER_SUBSCRIBER_NAME_SUFFIX , 
  OTHER_SUBSCRIBER_ID_QLFR , 
  OTHER_SUBSCRIBER_ID , 
  OTHER_SUBSCRIBER_ADDRESS_1 , 
  OTHER_SUBSCRIBER_ADDRESS_2 , 
  OTHER_SUBSCRIBER_CITY , 
  OTHER_SUBSCRIBER_STATE , 
  OTHER_SUBSCRIBER_ZIP ,
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
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_INST_SUBSCRIBER) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
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
  
Loop2000B_SBR AS 
(
select 
XML_MD5,
 
XMLGET( SBR.value, ''payerResponsibilityCode'' ):"$"::VARCHAR  As payer_responsibility_code,
XMLGET( SBR.value , ''subscriberRelationshipCode'' ):"$"::VARCHAR  As subscriber_relationship_code,
XMLGET(SBR.value , ''subscriberPolicyNumber'' ):"$"::VARCHAR  As subscriber_policy_number,
XMLGET( SBR.value, ''subscriberPlanName'' ):"$"::VARCHAR  As subscriber_plan_name,
null as subscriber_insurance_type,
null as subscriber_cob_code,
null as yes_no_response_code,
null as subscriber_employement_status,
XMLGET( SBR.value , ''clmFillingIndicatorCode'' ):"$"::VARCHAR  As claim_type

 
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) SBR
where segment = ''Loop2000B''
  and (GET(SBR.value, ''@'') = ''SBR'' OR SBR.value = ''SBR'')
)


 ,


 
Loop2320_SBR AS 
(
select 
XML_MD5,segments.IDX, 
 
XMLGET( SBR.value, ''otherPayerResponsibilityCode'' ):"$"::VARCHAR  As     other_payer_responsibility_code,
XMLGET( SBR.value , ''otherSubscriberRelationshipCode'' ):"$"::VARCHAR  As other_subscriber_relationship_code,
XMLGET(SBR.value , ''otherSubscriberPolicyNumber'' ):"$"::VARCHAR  As      other_subscriber_policy_number,
XMLGET( SBR.value, ''otherSubscriberPlanName'' ):"$"::VARCHAR  As          other_subscriber_plan_name,
XMLGET( SBR.value, ''clmFillingIndicatorCode'' ):"$"::VARCHAR  As          other_clm_filling_ind_cd  

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) SBR
where segment = ''Loop2320'' 
  and (GET(SBR.value, ''@'') = ''SBR'' OR SBR.value = ''SBR'')
)
 

 
,

 
 
Loop2010BA_NM1 AS 
(
select 
XML_MD5, 
XMLGET(NM1.value, ''subscriberTypeQlfr'' ):"$"::INTEGER  As subscriber_type,
XMLGET(NM1.value, ''subscriberNameLast'' ):"$"::VARCHAR  As subscriber_name_last,
XMLGET(NM1.value , ''subscriberNameFirst'' ):"$"::VARCHAR  As subscriber_name_first,
XMLGET( NM1.value, ''subscriberNameMiddle'' ):"$"::VARCHAR  As subscriber_name_middle,
null as   name_prefix,
XMLGET( NM1.value, ''subscriberNameSuffix'' ):"$"::VARCHAR  As name_suffix,
XMLGET( NM1.value, ''subscriberIdQlfr'' ):"$"::VARCHAR  As subscriber_id_qualifier,
XMLGET( NM1.value , ''subscriberId'' ):"$"::VARCHAR  As subscriber_id
 
from segments, LATERAL FLATTEN(to_array(get(xml,''$''))) NM1
where segment = ''Loop2010BA''
    and (GET(NM1.value, ''@'') = ''NM1'' )
 
)
 

,
 
 
Loop2010BA_N3 AS 
(
select 
XML_MD5, 
XMLGET(N3.value, ''subscriberAddress1'' ):"$"::VARCHAR  As subscriber_address1,
XMLGET(N3.value , ''subscriberAddress2'' ):"$"::VARCHAR  As subscriber_address2
 
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) N3
where segment = ''Loop2010BA''
 
      and (GET(N3.value, ''@'') = ''N3'' )
)
 

,
 
Loop2010BA_N4 AS 
(
select 
XML_MD5, 
XMLGET(N4.value , ''subscriberCity'' ):"$"::VARCHAR  As subscriber_city,
XMLGET(N4.value, ''subscriberState'' ):"$"::VARCHAR  As subscriber_state,
XMLGET(N4.value, ''subscriberZip'' ):"$"::VARCHAR  As subscriber_postalcode
 
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) N4
where segment = ''Loop2010BA''
 
        and (GET(N4.value, ''@'') = ''N4'')
)
 

,
Loop2010BA_DMG AS 
(
select 
XML_MD5, 
XMLGET( DMG.value, ''subscriberDateOfBirth'' ):"$"::VARCHAR  As subscriber_dateofbirth,
XMLGET( DMG.value, ''subscriberGenderCode'' ):"$"::VARCHAR  As subscriber_gendercode
 
 
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) DMG
where segment = ''Loop2010BA''
and (GET(DMG.value, ''@'') = ''DMG'' )
)
 
 

 
,
 
Loop2320_Seq AS 
(
select 
XML_MD5, idx 
from segments
where segment = ''Loop2320''
 
)
 


,
 
Loop2320_Loop2330A_NM1 AS 
(
select 
XML_MD5, segments.IDX, 
XMLGET( NM1.value, ''otherSubscriberTypeQlfr'' ):"$"::INTEGER  As    other_subscriber_type,
XMLGET( NM1.value, ''otherSubscriberNameLast'' ):"$"::VARCHAR  As    other_subscriber_name_last,
XMLGET( NM1.value , ''otherSubscriberNameFirst'' ):"$"::VARCHAR  As  other_subscriber_name_first,
XMLGET( NM1.value , ''otherSubscriberNameMiddle'' ):"$"::VARCHAR  As other_subscriber_name_middle,
XMLGET( NM1.value, ''otherSubscriberNameSuffix'' ):"$"::VARCHAR  As  other_subscriber_suffix,
XMLGET( NM1.value , ''otherSubscriberIdQlfr'' ):"$"::VARCHAR  As     other_subscriber_id_qlfr,
XMLGET( NM1.value, ''otherSubscriberId'' ):"$"::VARCHAR  As          other_subscriber_id
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2330A, LATERAL FLATTEN(to_array(GET(Loop2330A.value,''$''))) NM1
where segment = ''Loop2320''
and (GET(Loop2330A.value, ''@'') = ''Loop2330A'' )
and (GET(NM1.value, ''@'') = ''NM1'' )
)
 

 
,
 
Loop2320_Loop2330A_N3 AS 
(
select 
XML_MD5, segments.IDX,
  XMLGET( N3.value, ''otherSubscriberAddress1'' ):"$"::VARCHAR  As other_subscriber_address1,
  XMLGET( N3.value, ''otherSubscriberAddress2'' ):"$"::VARCHAR  As other_subscriber_address2
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2330A,LATERAL FLATTEN(to_array(GET(Loop2330A.value,''$''))) N3
where segment = ''Loop2320''
and (GET(Loop2330A.value, ''@'') = ''Loop2330A'' )
and (GET(N3.value, ''@'') = ''N3'')
 
)
 
 
 
,
Loop2320_Loop2330A_N4 AS 
(
select 
XML_MD5, segments.IDX,
   XMLGET( N4.value, ''otherSubscriberCity'' ):"$"::VARCHAR  As   other_subscriber_city,
   XMLGET( N4.value , ''otherSubscriberState'' ):"$"::VARCHAR  As other_subscriber_state,
   XMLGET( N4.value , ''otherSubscriberZip'' ):"$"::VARCHAR  As   other_subscriber_zip
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2330A,LATERAL FLATTEN(to_array(GET(Loop2330A.value,''$''))) N4
where segment = ''Loop2320''
and (GET(Loop2330A.value, ''@'') = ''Loop2330A'' )
       and (GET(N4.value, ''@'') = ''N4'' )
)
 
,

Loop2320 AS
(
select Loop2320_Seq.XML_MD5, 
  Loop2320_Loop2330A_NM1.other_subscriber_type,
  Loop2320_Loop2330A_NM1.other_subscriber_name_last,
  Loop2320_Loop2330A_NM1.other_subscriber_name_first,
  Loop2320_Loop2330A_NM1.other_subscriber_name_middle,
  Loop2320_Loop2330A_NM1.other_subscriber_suffix,
  Loop2320_Loop2330A_NM1.other_subscriber_id_qlfr,
  Loop2320_Loop2330A_NM1.other_subscriber_id,
   Loop2320_Loop2330A_N3.other_subscriber_address1,
  Loop2320_Loop2330A_N3.other_subscriber_address2,
  Loop2320_Loop2330A_N4.other_subscriber_city,
  Loop2320_Loop2330A_N4.other_subscriber_state,
  Loop2320_Loop2330A_N4.other_subscriber_zip,
  Loop2320_SBR.other_payer_responsibility_code,
Loop2320_SBR.other_subscriber_relationship_code,
  Loop2320_SBR.other_subscriber_policy_number,
Loop2320_SBR.other_subscriber_plan_name,
Loop2320_SBR.other_clm_filling_ind_cd  
   from Loop2320_Seq 
LEFT JOIN Loop2320_Loop2330A_NM1 ON Loop2320_Seq.XML_MD5 = Loop2320_Loop2330A_NM1.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_Loop2330A_NM1.IDX
LEFT JOIN Loop2320_Loop2330A_N3 on Loop2320_Seq.XML_MD5 = Loop2320_Loop2330A_N3.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_Loop2330A_N3.IDX
  LEFT JOIN Loop2320_Loop2330A_N4 on Loop2320_Seq.XML_MD5 = Loop2320_Loop2330A_N4.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_Loop2330A_N4.IDX
  LEFT JOIN Loop2320_SBR on Loop2320_Seq.XML_MD5 = Loop2320_SBR.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_SBR.IDX
where (Loop2320_Loop2330A_NM1.XML_MD5 is not null or Loop2320_Loop2330A_N3.XML_MD5 is not null or Loop2320_Loop2330A_N4.XML_MD5 is not null or Loop2320_SBR.XML_MD5 is not null )
) 



 
select  
 

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
 

Loop2000B_SBR.payer_responsibility_code,
Loop2000B_SBR.subscriber_relationship_code,
Loop2000B_SBR.subscriber_policy_number,
Loop2000B_SBR.subscriber_plan_name,
Loop2000B_SBR.subscriber_insurance_type,
Loop2000B_SBR.subscriber_cob_code,
Loop2000B_SBR.yes_no_response_code,
Loop2000B_SBR.subscriber_employement_status,
-----------
Loop2000B_SBR.claim_type,
 
------
 
Loop2010BA_NM1.subscriber_type,
Loop2010BA_NM1.subscriber_name_last,
Loop2010BA_NM1.subscriber_name_first,
Loop2010BA_NM1.subscriber_name_middle,
Loop2010BA_NM1.name_prefix,
Loop2010BA_NM1.name_suffix,
Loop2010BA_NM1.subscriber_id_qualifier,
Loop2010BA_NM1.subscriber_id,
-----
Loop2010BA_N3.subscriber_address1,
Loop2010BA_N3.subscriber_address2,
 
 
-----
 
Loop2010BA_N4.subscriber_city,
Loop2010BA_N4.subscriber_state,
Loop2010BA_N4.subscriber_postalcode,
 
----
 
Loop2010BA_DMG.subscriber_dateofbirth,
Loop2010BA_DMG.subscriber_gendercode,
 
----
Loop2320.other_payer_responsibility_code,
Loop2320.other_subscriber_relationship_code,
Loop2320.other_subscriber_policy_number,
Loop2320.other_subscriber_plan_name,
Loop2320.other_clm_filling_ind_cd,

---------------------------------------

Loop2320.other_subscriber_type,
Loop2320.other_subscriber_name_last,
Loop2320.other_subscriber_name_first,
Loop2320.other_subscriber_name_middle,
Loop2320.other_subscriber_suffix,
Loop2320.other_subscriber_id_qlfr,
Loop2320.other_subscriber_id,
 
 
Loop2320.other_subscriber_address1,
Loop2320.other_subscriber_address2,
 
Loop2320.other_subscriber_city,
Loop2320.other_subscriber_state,
Loop2320.other_subscriber_zip,
 
 
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
     LEFT JOIN Loop2000B_SBR ON GS.XML_MD5 = Loop2000B_SBR.XML_MD5
     LEFT JOIN Loop2010BA_NM1 on GS.XML_MD5 = Loop2010BA_NM1.XML_MD5
     LEFT JOIN Loop2010BA_N3 on GS.XML_MD5 = Loop2010BA_N3.XML_MD5
     LEFT JOIN Loop2010BA_N4 on GS.XML_MD5 = Loop2010BA_N4.XML_MD5
     LEFT JOIN Loop2010BA_DMG on GS.XML_MD5 = Loop2010BA_DMG.XML_MD5
 
     LEFT JOIN Loop2320 on GS.XML_MD5 = Loop2320.XML_MD5 

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
