USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_PROF_PATIENT"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''PROF_PATIENT'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_PROF_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.PROF_CLAIMS_RAW'';

V_PROF_PATIENT VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.PROF_PATIENT'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD PROF_PATIENT'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_PROF_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );

INSERT INTO IDENTIFIER(:V_PROF_PATIENT)
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
  PAT_DEATH_DT , 
  PAT_WEIGHT_MEASURE_UNIT , 
  PAT_WEIGHT , 
  PAT_PREGNANCY_INDICATOR , 
  PAT_INSURER_RELATIONSHIP_CD , 
  PAT_DEP_DEATH_DT , 
  PAT_DEP_WEIGHT_MEASURE_UNIT , 
  PAT_DEP_WEIGHT , 
  PAT_DEP_PREGNANCY_INDICATOR , 
  PAT_TYPE_QLFR , 
  PAT_NAME_LAST , 
  PAT_NAME_FIRST , 
  PAT_NAME_MIDDLE , 
  PAT_NAME_SUFFIX , 
  PAT_ADDRESS_1 , 
  PAT_ADDRESS_2 , 
  PAT_CITY , 
  PAT_STATE , 
  PAT_ZIP , 
  PAT_DATEOFBIRTH , 
  PAT_GENDERCODE ,
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
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_PROF_PATIENT) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
   QUALIFY ROW_NUMBER() OVER  (PARTITION BY XML_MD5 ORDER BY 1) = 1

)
,


segments as (
select   XML_MD5, value as XML, SUBSTR(VALUE, 2, regexp_instr(VALUE, ''>'')-2) AS segment, FILE_SOURCE, FILE_NAME
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


Loop2000B_PAT AS 
(
select 
XML_MD5, 
XMLGET ( PAT.value,''subscriberDeathDate'' ): "$"::VARCHAR AS pat_death_dt,
XMLGET ( PAT.value,''subscriberWeightMeasureUnit'' ): "$"::VARCHAR AS pat_weight_measure_unit,
XMLGET ( PAT.value,''subscriberWeight'' ): "$"::VARCHAR AS pat_weight,
XMLGET ( PAT.value,''subscriberPregnancyIndicator'' ): "$"::VARCHAR AS pat_pregnancy_indicator


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) PAT
where segment = ''Loop2000B''
and GET(PAT.value, ''@'') = ''PAT''
)


,


Loop2000C_PAT AS 
(
select
XML_MD5, 
XMLGET ( PAT.VALUE,''patInsurerRelationshipCode'' ): "$"::VARCHAR AS pat_insurer_relationship_cd,
XMLGET ( PAT.VALUE,''patDeathDate'' ): "$"::VARCHAR AS pat_dep_death_dt,
XMLGET ( PAT.VALUE,''patWeightMeasureUnit'' ): "$"::VARCHAR AS pat_dep_weight_measure_unit,
XMLGET ( PAT.VALUE,''patWeight'' ): "$"::VARCHAR AS pat_dep_weight,
XMLGET ( PAT.VALUE,''patPregnancyIndicator'' ): "$"::VARCHAR AS pat_dep_pregnancy_indicator

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) PAT
where segment = ''Loop2000C''
and GET(PAT.value, ''@'') = ''PAT''
)


,


Loop2010CA_NM1 AS 
(
select 
XML_MD5, 
XMLGET ( NM1.value,''patTypeQlfr'' ): "$"::VARCHAR AS pat_type_qlfr,
XMLGET ( NM1.value,''patNameLast'' ): "$"::VARCHAR AS pat_name_last,
XMLGET ( NM1.value,''patNameFirst'' ): "$"::VARCHAR AS pat_name_first,
XMLGET ( NM1.value,''patNameMiddle'' ): "$"::VARCHAR AS pat_name_middle,
XMLGET ( NM1.value,''patNameSuffix'' ): "$"::VARCHAR AS pat_name_suffix



from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) NM1
where segment = ''Loop2010CA''
and GET(NM1.value, ''@'') = ''NM1''
)

,


Loop2010CA_N3 AS 
(
select 
XML_MD5, 
XMLGET ( N3.value,''patAddress1'' ): "$"::VARCHAR AS pat_address_1,
XMLGET ( N3.value,''patAddress2'' ): "$"::VARCHAR AS pat_address_2




from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) N3
where segment = ''Loop2010CA''
and GET(N3.value, ''@'') = ''N3''
)

,


Loop2010CA_N4 AS 
(
select 
XML_MD5, 
XMLGET ( N4.value,''patCity'' ): "$"::VARCHAR AS pat_city,
XMLGET ( N4.value,''patState'' ): "$"::VARCHAR AS pat_state,
XMLGET ( N4.value,''patZip'' ): "$"::VARCHAR AS pat_zip




from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) N4
where segment = ''Loop2010CA''
and GET(N4.value, ''@'') = ''N4''
)

,


Loop2010CA_DMG AS 
(
select 
XML_MD5, 
XMLGET ( DMG.value,''patDateOfBirth'' ): "$"::VARCHAR AS pat_dateofbirth,
XMLGET ( DMG.value,''patGenderCode'' ): "$"::VARCHAR AS pat_gendercode




from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) DMG
where segment = ''Loop2010CA''
and GET(DMG.value, ''@'') = ''DMG''
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


  Loop2000B_PAT.PAT_DEATH_DT , 
  Loop2000B_PAT.PAT_WEIGHT_MEASURE_UNIT , 
  Loop2000B_PAT.PAT_WEIGHT , 
  Loop2000B_PAT.PAT_PREGNANCY_INDICATOR , 
  ----------
  Loop2000C_PAT.PAT_INSURER_RELATIONSHIP_CD , 
  Loop2000C_PAT.PAT_DEP_DEATH_DT , 
  Loop2000C_PAT.PAT_DEP_WEIGHT_MEASURE_UNIT , 
  Loop2000C_PAT.PAT_DEP_WEIGHT , 
  Loop2000C_PAT.PAT_DEP_PREGNANCY_INDICATOR , 
  -------
  Loop2010CA_NM1.PAT_TYPE_QLFR , 
  Loop2010CA_NM1.PAT_NAME_LAST , 
  Loop2010CA_NM1.PAT_NAME_FIRST , 
  Loop2010CA_NM1.PAT_NAME_MIDDLE , 
  Loop2010CA_NM1.PAT_NAME_SUFFIX , 
  ----------
  Loop2010CA_N3.PAT_ADDRESS_1 , 
  Loop2010CA_N3.PAT_ADDRESS_2 , 
  --------
  Loop2010CA_N4.PAT_CITY , 
  Loop2010CA_N4.PAT_STATE , 
  Loop2010CA_N4.PAT_ZIP , 
  --------
  Loop2010CA_DMG.PAT_DATEOFBIRTH , 
  Loop2010CA_DMG.PAT_GENDERCODE ,
  ------

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
LEFT JOIN Loop2000B_PAT  ON GS.XML_MD5 = Loop2000B_PAT.XML_MD5
LEFT JOIN Loop2000C_PAT  ON GS.XML_MD5 = Loop2000C_PAT.XML_MD5
LEFT JOIN Loop2010CA_NM1  ON GS.XML_MD5 = Loop2010CA_NM1.XML_MD5
LEFT JOIN Loop2010CA_N3  ON GS.XML_MD5 = Loop2010CA_N3.XML_MD5
LEFT JOIN Loop2010CA_N4  ON GS.XML_MD5 = Loop2010CA_N4.XML_MD5
LEFT JOIN Loop2010CA_DMG  ON GS.XML_MD5 = Loop2010CA_DMG.XML_MD5
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
