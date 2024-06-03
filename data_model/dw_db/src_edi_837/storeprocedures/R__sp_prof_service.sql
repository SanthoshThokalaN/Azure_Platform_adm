USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_PROF_SERVICE"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''PROF_SERVICE'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_PROF_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.PROF_CLAIMS_RAW'';

V_PROF_SERVICE VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.PROF_SERVICE'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD PROF_SERVICE'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_PROF_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );

INSERT INTO IDENTIFIER(:V_PROF_SERVICE)
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
  SL_SEQ_NUM , 
  DMEC_MNI_CERT_TRANSMISSION_CD , 
  DMEC_TYPE_CD , 
  DMEC_DURATION_MEASURE_UNIT , 
  DMEC_DURATION , 
  DMEC_MNI_CONDITION_INDICATOR , 
  DMEC_MNI_CONDITION_CD_1 , 
  DMEC_MNI_CONDITION_CD_2 , 
  TEST_MEASURE_RESULTS , 
  CLINCAL_LAB_IMPRVMENT_AMNDMENT_NUM , 
  REFERRING_CLIA_NUMBER , 
  MEDICAL_PROCEDURE_ID , 
  MEDICAL_NECESSITY_MEASURE_UNIT , 
  MEDICAL_NECESSITY_LENGTH , 
  DME_RENTAL_PRICE , 
  DME_PURCHASE_PRICE , 
  DME_FREQUENCY_CD , 
  SV_FACILITY_TYPE_QLFR , 
  SV_FACILITY_NAME , 
  SV_FACILITY_PRIMARY_ID , 
  SV_FACILITY_ADDR_1 , 
  SV_FACILITY_ADDR_2 , 
  SV_FACILITY_CITY , 
  SV_FACILITY_STATE , 
  SV_FACILITY_ZIP , 
  SV_FACILITY_REF_ID_QLFR_1 , 
  SV_FACILITY_SECONDARY_ID_1 , 
  SV_FACILITY_SECONDARY_ID_2 , 
  DRUG_PRODUCT_ID_QLFR , 
  DRUG_PRODUCT_ID , 
  DRUG_UNIT_COUNT , 
  DRUG_MEASURE_UNIT ,
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
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_PROF_SERVICE) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
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
XMLGET( HL.value, ''prvHlNumber'' ):"$"::VARCHAR  As provider_hl_no,
XMLGET( HL.value, ''subscriberHlNumber'' ):"$"::VARCHAR  As subscriber_hl_no,
XMLGET( HL.value , ''subscriberHlNumber'' ):"$"::VARCHAR  As payer_hl_no
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HL
where segment = ''Loop2000B''
and (GET(HL.value, ''@'') = ''HL'')
)



,
Loop2300_CLM AS 
(
select 
XML_MD5, 
XMLGET( CLM.value, ''patControlNumber'' ):"$"::VARCHAR  As claim_id

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) CLM
where segment = ''Loop2300'' 
and (GET(CLM.value, ''@'') = ''CLM'')
)



,

Loop2400_LX AS 
(
select 
XML_MD5, 
XMLGET( LX.value, ''svSeqNumber'' ):"$"::INTEGER  As sl_seq_num
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) LX
where segment = ''Loop2400'' 
and (GET(LX.value, ''@'') = ''LX'')
)



,

Loop2400_PWK AS 
(
select 
XML_MD5, 
  XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET( PWK.value, ''svDmeMniCertTransmissionCode'' ):"$"::VARCHAR  As dmec_mni_cert_transmission_cd

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) PWK
where segment = ''Loop2400'' 
and (GET(PWK.value, ''@'') = ''PWK'')
)

,

Loop2400_CR3 AS 
(
select 
XML_MD5, 
  XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET( CR3.value, ''svDmeTypeCode'' ):"$"::VARCHAR  As dmec_type_cd,
XMLGET( CR3.value, ''svDmeDurationMeasureUnit'' ):"$"::VARCHAR  As dmec_duration_measure_unit,
XMLGET( CR3.value, ''svDmeDuration'' ):"$"::VARCHAR  As dmec_duration
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) CR3
where segment = ''Loop2400'' 
and (GET(CR3.value, ''@'') = ''CR3'')
)




,


Loop2400_CRC_09 AS 
(
select 
XML_MD5, 
  XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET( CRC_09.value, ''svDmeMniResponseCode'' ):"$"::VARCHAR  As dmec_mni_condition_indicator,
XMLGET( CRC_09.value, ''svDmeMniConditionIndicator1'' ):"$"::VARCHAR  As dmec_mni_condition_cd_1,
XMLGET( CRC_09.value, ''svDmeMniConditionIndicator2'' ):"$"::VARCHAR  As dmec_mni_condition_cd_2
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) CRC_09
where segment = ''Loop2400'' 
and (GET(CRC_09.value, ''@'') = ''CRC_09'')
)

,

Loop2400_MEA_TR AS 
(
select 
XML_MD5, 
  XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
  array_construct(
        object_construct(''test_measure_ref_id'', ''TR'', ''test_measure_type'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureType1'' ): "$"::VARCHAR),''''),''test_measure_results'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureResults1'' ): "$"::VARCHAR),'''')),
        object_construct(''test_measure_ref_id'', ''TR'', ''test_measure_type'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureType2'' ): "$"::VARCHAR),''''),''test_measure_results'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureResults2'' ): "$"::VARCHAR),'''')),
        object_construct(''test_measure_ref_id'', ''TR'', ''test_measure_type'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureType3'' ): "$"::VARCHAR),''''),''test_measure_results'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureResults3'' ): "$"::VARCHAR),'''')),
        object_construct(''test_measure_ref_id'', ''TR'', ''test_measure_type'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureType4'' ): "$"::VARCHAR),''''),''test_measure_results'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureResults4'' ): "$"::VARCHAR),'''')),
        object_construct(''test_measure_ref_id'', ''TR'', ''test_measure_type'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureType5'' ): "$"::VARCHAR),''''),''test_measure_results'', coalesce((XMLGET ( MEA_TR.value,''svTestMeasureResults5'' ): "$"::VARCHAR),''''))
        
    ) AS test_measure_results

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) MEA_TR
where segment = ''Loop2400''
and GET(MEA_TR.value, ''@'') = ''MEA_TR''
)


,

Loop2400_REF_X4 AS 
(
select 
XML_MD5, 
  XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET( REF_X4.value, ''svClincalLabImprvmentAmndmentNumber'' ):"$"::VARCHAR  As clincal_lab_imprvment_amndment_num
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_X4
where segment = ''Loop2400'' 
and (GET(REF_X4.value, ''@'') = ''REF_X4'')
)



,

Loop2400_REF_F4 AS 
(
select 
XML_MD5, 
  XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET( REF_F4.value, ''svReferringCliaNumber'' ):"$"::VARCHAR  As referring_clia_number
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_F4
where segment = ''Loop2400'' 
and (GET(REF_F4.value, ''@'') = ''REF_F4'')
)



,
Loop2400_SV5 AS 
(
select 
XML_MD5,
  XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET( SV5.value, ''svMedicalProcedureId'' ):"$"::VARCHAR  As medical_procedure_id,
XMLGET( SV5.value, ''svMedicalNecessityMeasureUnit'' ):"$"::VARCHAR  As medical_necessity_measure_unit,
XMLGET( SV5.value, ''svMedicalNecessityLength'' ):"$"::VARCHAR  As medical_necessity_length,
  XMLGET( SV5.value, ''svDmeRentalPrice'' ):"$"::VARCHAR  As dme_rental_price,
  XMLGET( SV5.value, ''svDmePurchasePrice'' ):"$"::VARCHAR  As dme_purchase_price,
  XMLGET( SV5.value, ''svDmeFrequencyCode'' ):"$"::VARCHAR  As dme_frequency_cd
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) SV5
where segment = ''Loop2400'' 
and (GET(SV5.value, ''@'') = ''SV5'')
)

,


Loop2400_Loop2420C_NM1 AS 
(
select 
XML_MD5, 
  XMLGET( Loop2420C.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,   
XMLGET( NM1.value, ''svFacilityTypeQlfr'' ):"$"::VARCHAR  As sv_facility_type_qlfr,
XMLGET( NM1.value, ''svFacilityName'' ):"$"::VARCHAR  As sv_facility_name,
XMLGET( NM1.value, ''svFacilityPrimaryId'' ):"$"::VARCHAR  As sv_facility_primary_id 
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2420C ,LATERAL FLATTEN(to_array(GET(Loop2420C.value,''$''))) NM1
where segment = ''Loop2400'' 
and (GET(Loop2420C.value, ''@'') = ''Loop2420C'')
     and GET( NM1.value, ''@'') = ''NM1''

)


,


Loop2400_Loop2420C_N3 AS 
(
select 
XML_MD5, 
    XMLGET( Loop2420C.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,   
XMLGET( N3.value, ''svFacilityAddress1'' ):"$"::VARCHAR  As sv_facility_addr_1,
XMLGET( N3.value, ''svFacilityAddress2'' ):"$"::VARCHAR  As sv_facility_addr_2
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2420C ,LATERAL FLATTEN(to_array(GET(Loop2420C.value,''$''))) N3
where segment = ''Loop2400'' 
and (GET(Loop2420C.value, ''@'') = ''Loop2420C'')
     and GET( N3.value, ''@'') = ''N3''

)


,

Loop2400_Loop2420C_N4 AS 
(
select 
XML_MD5, 
    XMLGET( Loop2420C.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,   
XMLGET( N4.value, ''svFacilityCity'' ):"$"::VARCHAR  As sv_facility_city,
XMLGET( N4.value, ''svFacilityState'' ):"$"::VARCHAR  As sv_facility_state,
XMLGET( N4.value, ''svFacilityZip'' ):"$"::VARCHAR  As sv_facility_zip 
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2420C ,LATERAL FLATTEN(to_array(GET(Loop2420C.value,''$''))) N4
where segment = ''Loop2400'' 
and (GET(Loop2420C.value, ''@'') = ''Loop2420C'')
     and GET( N4.value, ''@'') = ''N4''

)
,


Loop2400_Loop2420C_REF_G2_1 AS 
(
select 
XML_MD5, 
    XMLGET( Loop2420C.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,   
XMLGET( REF_G2.value, ''svFacilitySecondId1'' ):"$"::VARCHAR  As sv_facility_secondary_id_1,
XMLGET( REF_G2.value, ''svFacilityOtherSecondId1'' ):"$"::VARCHAR  As sv_facility_secondary_id_2

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2420C ,LATERAL FLATTEN(to_array(GET(Loop2420C.value,''$''))) REF_G2
where segment = ''Loop2400'' 
and (GET(Loop2420C.value, ''@'') = ''Loop2420C'')
     and GET( REF_G2.value, ''@'') = ''REF_G2''

)

,

Loop2400_Loop2420C_REF_G2_2 AS 
(
select 
XML_MD5, 
    XMLGET( Loop2420C.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,   
XMLGET( REF_G2.value, ''svFacilitySecondId2'' ):"$"::VARCHAR  As sv_facility_secondary_id_1,
XMLGET( REF_G2.value, ''svFacilityOtherSecondId2'' ):"$"::VARCHAR  As sv_facility_secondary_id_2

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2420C ,LATERAL FLATTEN(to_array(GET(Loop2420C.value,''$''))) REF_G2
where segment = ''Loop2400'' 
and (GET(Loop2420C.value, ''@'') = ''Loop2420C'')
     and GET( REF_G2.value, ''@'') = ''REF_G2''

)
,

Loop2400_Loop2420C_REF_G2_3 AS 
(
select 
XML_MD5, 
    XMLGET( Loop2420C.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,   
XMLGET( REF_G2.value, ''svFacilitySecondId3'' ):"$"::VARCHAR  As sv_facility_secondary_id_1,
XMLGET( REF_G2.value, ''svFacilityOtherSecondId3'' ):"$"::VARCHAR  As sv_facility_secondary_id_2

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2420C ,LATERAL FLATTEN(to_array(GET(Loop2420C.value,''$''))) REF_G2
where segment = ''Loop2400'' 
and (GET(Loop2420C.value, ''@'') = ''Loop2420C'')
     and GET( REF_G2.value, ''@'') = ''REF_G2''

)
,

Loop2400_Loop2420C_REF_G2 (XML_MD5,sl_seq_num, sv_facility_secondary_id_1, sv_facility_secondary_id_2) AS 

(
  
  
select XML_MD5, sl_seq_num,sv_facility_secondary_id_1, sv_facility_secondary_id_2 from Loop2400_Loop2420C_REF_G2_1  where  (sv_facility_secondary_id_1 is not null or  sv_facility_secondary_id_2 is not null)
UNION ALL
select XML_MD5,sl_seq_num, sv_facility_secondary_id_1, sv_facility_secondary_id_2 from Loop2400_Loop2420C_REF_G2_2 where  (sv_facility_secondary_id_1 is not null or  sv_facility_secondary_id_2 is not null)
UNION ALL
select XML_MD5, sl_seq_num,sv_facility_secondary_id_1, sv_facility_secondary_id_2 from Loop2400_Loop2420C_REF_G2_3 where  (sv_facility_secondary_id_1 is not null or  sv_facility_secondary_id_2 is not null)


)

,
Loop2400_Loop2410_LIN AS 
(
select 
XML_MD5, 
    XMLGET( Loop2410.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,   
XMLGET( LIN.value, ''svDrugProductIdQlfr'' ):"$"::VARCHAR  As drug_product_id_qlfr,
XMLGET( LIN.value, ''svDrugProductId'' ):"$"::VARCHAR  As drug_product_id
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2410 ,LATERAL FLATTEN(to_array(GET(Loop2410.value,''$''))) LIN
where segment = ''Loop2400'' 
and (GET(Loop2410.value, ''@'') = ''Loop2410'')
     and GET( LIN.value, ''@'') = ''LIN''

)

,
Loop2400_Loop2410_CTP AS 
(
select 
XML_MD5, 
      XMLGET( Loop2410.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,   
XMLGET( CTP.value, ''svDrugUnitCount'' ):"$"::VARCHAR  As drug_unit_count,
XMLGET( CTP.value, ''svDrugMeasureUnit'' ):"$"::VARCHAR  As drug_measure_unit
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2410 ,LATERAL FLATTEN(to_array(GET(Loop2410.value,''$''))) CTP
where segment = ''Loop2400'' 
and (GET(Loop2410.value, ''@'') = ''Loop2410'')
     and GET( CTP.value, ''@'') = ''CTP''

)



,

Loop2400 AS 
(
select 
Loop2400_LX.XML_MD5,Loop2400_LX.sl_seq_num,
Loop2400_PWK.dmec_mni_cert_transmission_cd,
Loop2400_CR3.dmec_type_cd,Loop2400_CR3.dmec_duration_measure_unit,Loop2400_CR3.dmec_duration,
Loop2400_CRC_09.dmec_mni_condition_indicator, Loop2400_CRC_09.dmec_mni_condition_cd_1,Loop2400_CRC_09.dmec_mni_condition_cd_2,
Loop2400_MEA_TR.test_measure_results,
Loop2400_REF_X4.clincal_lab_imprvment_amndment_num,
  Loop2400_REF_F4.referring_clia_number,
  Loop2400_SV5.medical_procedure_id,
Loop2400_SV5.medical_necessity_measure_unit,Loop2400_SV5.medical_necessity_length,Loop2400_SV5.dme_rental_price,
Loop2400_SV5.dme_purchase_price,Loop2400_SV5.dme_frequency_cd,
Loop2400_Loop2420C_NM1.sv_facility_type_qlfr,Loop2400_Loop2420C_NM1.sv_facility_name,Loop2400_Loop2420C_NM1.sv_facility_primary_id,
Loop2400_Loop2420C_N3.sv_facility_addr_1,Loop2400_Loop2420C_N3.sv_facility_addr_2,
Loop2400_Loop2420C_N4.sv_facility_city,Loop2400_Loop2420C_N4.sv_facility_state,Loop2400_Loop2420C_N4.sv_facility_zip,
Loop2400_Loop2410_LIN.drug_product_id_qlfr,Loop2400_Loop2410_LIN.drug_product_id,
Loop2400_Loop2410_CTP.drug_unit_count,Loop2400_Loop2410_CTP.drug_measure_unit,
  Loop2400_Loop2420C_REF_G2.sv_facility_secondary_id_1,Loop2400_Loop2420C_REF_G2.sv_facility_secondary_id_2

from Loop2400_LX
LEFT JOIN Loop2400_PWK ON Loop2400_LX.XML_MD5 = Loop2400_PWK.XML_MD5 AND Loop2400_PWK.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_CR3 ON Loop2400_LX.XML_MD5 = Loop2400_CR3.XML_MD5 AND Loop2400_CR3.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_CRC_09 ON Loop2400_LX.XML_MD5 = Loop2400_CRC_09.XML_MD5 AND Loop2400_CRC_09.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_MEA_TR ON Loop2400_LX.XML_MD5 = Loop2400_MEA_TR.XML_MD5 AND Loop2400_MEA_TR.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_REF_X4 ON Loop2400_LX.XML_MD5 = Loop2400_REF_X4.XML_MD5 AND Loop2400_REF_X4.sl_seq_num = Loop2400_LX.sl_seq_num
 LEFT JOIN Loop2400_REF_F4 ON Loop2400_LX.XML_MD5 = Loop2400_REF_F4.XML_MD5 AND Loop2400_REF_F4.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_SV5 ON Loop2400_LX.XML_MD5 = Loop2400_SV5.XML_MD5 AND Loop2400_SV5.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_Loop2420C_NM1 ON  Loop2400_LX.XML_MD5 = Loop2400_Loop2420C_NM1.XML_MD5 AND Loop2400_Loop2420C_NM1.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_Loop2420C_N3 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2420C_N3.XML_MD5 AND Loop2400_Loop2420C_N3.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_Loop2420C_N4 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2420C_N4.XML_MD5 AND Loop2400_Loop2420C_N4.sl_seq_num = Loop2400_LX.sl_seq_num
  LEFT JOIN Loop2400_Loop2420C_REF_G2 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2420C_REF_G2.XML_MD5 AND Loop2400_Loop2420C_REF_G2.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_Loop2410_LIN ON Loop2400_LX.XML_MD5 = Loop2400_Loop2410_LIN.XML_MD5 AND Loop2400_Loop2410_LIN.sl_seq_num = Loop2400_LX.sl_seq_num
LEFT JOIN Loop2400_Loop2410_CTP ON Loop2400_LX.XML_MD5 = Loop2400_Loop2410_CTP.XML_MD5 AND Loop2400_Loop2410_CTP.sl_seq_num = Loop2400_LX.sl_seq_num)







select distinct

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

----

COALESCE(Loop2400.sl_seq_num,1) as sl_seq_num ,

------

Loop2400.dmec_mni_cert_transmission_cd,
Loop2400.dmec_type_cd,
Loop2400.dmec_duration_measure_unit,
Loop2400.dmec_duration,

-------
Loop2400.dmec_mni_condition_indicator,
Loop2400.dmec_mni_condition_cd_1,
Loop2400.dmec_mni_condition_cd_2,
------

Loop2400.test_measure_results,

------


Loop2400.clincal_lab_imprvment_amndment_num,

Loop2400.referring_clia_number,

--------

Loop2400.medical_procedure_id,
Loop2400.medical_necessity_measure_unit,
Loop2400.medical_necessity_length,
Loop2400.dme_rental_price,
Loop2400.dme_purchase_price,
Loop2400.dme_frequency_cd,

-------
Loop2400.sv_facility_type_qlfr,
Loop2400.sv_facility_name,
Loop2400.sv_facility_primary_id,

-----
Loop2400.sv_facility_addr_1,
Loop2400.sv_facility_addr_2,


------

Loop2400.sv_facility_city,


Loop2400.sv_facility_state,

Loop2400.sv_facility_zip,

------
''G2'' as sv_facility_ref_id_qlfr_1,
Loop2400.sv_facility_secondary_id_1,
Loop2400.sv_facility_secondary_id_2,

-----
Loop2400.drug_product_id_qlfr,
Loop2400.drug_product_id,
-----
Loop2400.drug_unit_count,

Loop2400.drug_measure_unit,
------
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
     LEFT JOIN Loop2000B_HL ON GS.XML_MD5 = Loop2000B_HL.XML_MD5
     left join Loop2300_CLM ON GS.XML_MD5 = Loop2300_CLM.XML_MD5
     left join Loop2400 on GS.XML_MD5 = Loop2400.XML_MD5 

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

