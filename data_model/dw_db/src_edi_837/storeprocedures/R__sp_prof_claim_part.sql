USE SCHEMA SRC_EDI_837;
 
CREATE OR REPLACE PROCEDURE "SP_PROF_CLAIM_PART"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE


V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''PROF_CLAIM_PART'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_PROF_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.PROF_CLAIMS_RAW'';

V_PROF_CLAIM_PART VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.PROF_CLAIM_PART'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD PROF_CLAIM_PART'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_PROF_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );

INSERT INTO identifier(:V_PROF_CLAIM_PART)
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
  TOTAL_CLAIM_CHARGE_AMT , 
  HEALTHCARESERVICE_LOCATION , 
  PROVIDER_ACCEPT_ASSIGN_CODE , 
  PROVIDER_BENEFIT_AUTH_CODE , 
  PROVIDER_PATINFO_RELEASE_AUTH_CODE , 
  DELAY_REASON_CODE , 
  HEALTH_CARE_CODE_INFO , 
  SV_REIMBURSEMENT_RATE , 
  SV_HCPCS_PAYABLE_AMT , 
  SV_CLM_PAYMENT_REMARK_CODE , 
  SL_SEQ_NUM , 
  PRODUCT_SERVICE_ID_QLFR , 
  LINE_ITEM_CHARGE_AMT , 
  MEASUREMENT_UNIT , 
  SERVICE_UNIT_COUNT , 
  DATE_TIME_QLFY , 
  DATE_TIME_FRMT_QLFY , 
  SERVICE_DATE , 
  CAS_ADJ_GROUP_CODE , 
  CAS_ADJ_REASON_CODE , 
  CAS_ADJ_AMT , 
  VENDOR_CD , 
  PROVIDER_SIGN_INDICATOR , 
  PAT_SIGNATURE_CD , 
  RELATED_CAUSE_CODE_INFO , 
  SPECIAL_PROGRAM_INDICATOR , 
  PATIENT_AMT_PAID , 
  SERVICE_AUTH_EXCEPTION_CODE , 
  CLINICAL_LAB_AMENDMENT_NUM , 
  NETWORK_TRACE_NUMBER , 
  MEDICAL_RECORD_NUMBER , 
  DEMONSTRATION_PROJECT_ID , 
  HEALTH_CARE_ADDITIONAL_CODE_INFO , 
  ANESTHESIA_PROCEDURE_CODE , 
  HC_CONDITION_CODES , 
  PAYER_CLM_CTRL_NUM , 
  NON_COVERED_CHARGE_AMT , 
  DIAGNOSIS_CODE_POINTERS , 
  EMERGENCY_INDICATOR , 
  FAMILY_PLANNING_INDICATOR , 
  EPSDT_INDICATOR , 
  CLM_BILLING_NOTE_TEXT ,
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
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_PROF_CLAIM_PART) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
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
XMLGET ( CLM.value,''patControlNumber'' ): "$"::VARCHAR AS claim_id,
XMLGET ( CLM.value,''clmTotalChargeAmt'' ): "$"::VARCHAR AS total_claim_charge_amt,
XMLGET ( CLM.value,''healthcareServiceLocation'' ): "$"::VARCHAR AS healthcareservice_location,
XMLGET ( CLM.value,''prvAcceptAssignCode'' ): "$"::VARCHAR AS provider_accept_assign_code,
XMLGET ( CLM.value,''prvBenefitAuthCode'' ): "$"::VARCHAR AS provider_benefit_auth_code,
XMLGET ( CLM.value,''prvPatInfoReleaseAuthCode'' ): "$"::VARCHAR AS provider_patinfo_release_auth_code,
XMLGET ( CLM.value,''delayReasonCode'' ): "$"::VARCHAR AS delay_reason_code,
XMLGET ( CLM.value,''prvSignIndicator'' ): "$"::VARCHAR AS provider_sign_indicator,
XMLGET ( CLM.value,''patSignatureCode'' ): "$"::VARCHAR AS pat_signature_cd,

XMLGET ( CLM.value,''relatedCausesCodeInfo1'' ): "$"::VARCHAR AS relatedCausesCodeInfo1, 
XMLGET ( CLM.value,''relatedCausesCodeInfo2'' ): "$"::VARCHAR AS relatedCausesCodeInfo2, 
XMLGET ( CLM.value,''stateCode'' ): "$"::VARCHAR AS stateCode, 
XMLGET ( CLM.value,''countryCode'' ): "$"::VARCHAR AS countryCode,
XMLGET ( CLM.value,''specialProgramIndicator'' ): "$"::VARCHAR AS specialProgramIndicator

from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) CLM
where segment = ''Loop2300''
and GET(CLM.value, ''@'') = ''CLM''
)

,


Loop2300_HI_BP_1 AS 
(
select 
XML_MD5, 
COALESCE(CONCAT(''BP:'', XMLGET ( HI_BP.value,''anesthesiaProcedureCode1'' ): "$"::VARCHAR), '''') AS anesthesiaProcedureCode1

from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_BP
where segment = ''Loop2300''
and GET(HI_BP.value, ''@'') = ''HI_BP''
)

,


Loop2300_HI_BP_2 AS 
(
select 
XML_MD5, 
XMLGET ( HI_BP.value,''anesthesiaProcedureCode2'' ): "$"::VARCHAR AS anesthesiaProcedureCode2

from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_BP
where segment = ''Loop2300''
and GET(HI_BP.value, ''@'') = ''HI_BP''
)
, 

Loop2300_HI_BP (XML_MD5, anesthesiaProcedureCode) as 

(
select XML_MD5, anesthesiaProcedureCode1 from Loop2300_HI_BP_1 where anesthesiaProcedureCode1 is not null
UNION ALL 
select XML_MD5, anesthesiaProcedureCode2 from Loop2300_HI_BP_2 where anesthesiaProcedureCode2 is not null
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

Loop2400_SV1 AS 
(
select 
XML_MD5, 
XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,

XMLGET ( SV1.value,''svProductServiceIdQlfr''): "$"::VARCHAR as svProductServiceIdQlfr,  
XMLGET ( SV1.value,''svProcedureCode''): "$"::VARCHAR as svProcedureCode,
XMLGET ( SV1.value,''svProcedureModifier1''): "$"::VARCHAR as svProcedureModifier1,
XMLGET ( SV1.value,''svProcedureModifier2''): "$"::VARCHAR as svProcedureModifier2,
XMLGET ( SV1.value,''svProcedureModifier3''): "$"::VARCHAR as svProcedureModifier3,
XMLGET ( SV1.value,''svProcedureModifier4''): "$"::VARCHAR as svProcedureModifier4,
XMLGET ( SV1.value,''svProcedureDesc''): "$"::VARCHAR as svProcedureDesc,
XMLGET ( SV1.value,''svLineItemChargeAmt'' ): "$"::VARCHAR AS line_item_charge_amt,
XMLGET ( SV1.value,''svMeasurementUnit'' ): "$"::VARCHAR AS measurement_unit,
XMLGET ( SV1.value,''svUnitCount'' ): "$"::VARCHAR AS service_unit_count,
XMLGET ( SV1.value,''svDiagnosisCodePointers'' ): "$"::VARCHAR AS diagnosis_code_pointers,
XMLGET ( SV1.value,''svEmergencyIndicator'' ): "$"::VARCHAR AS emergency_indicator,
XMLGET ( SV1.value,''svFamilyPlanningIndicator'' ): "$"::VARCHAR AS family_planning_indicator,
XMLGET ( SV1.value,''svEpsdtIndicator'' ): "$"::VARCHAR AS epsdt_indicator
from segments S , LATERAL FLATTEN(to_array(GET(xml,''$''))) SV1 
where segment = ''Loop2400''
and GET(SV1.value, ''@'') = ''SV1''
)
,

Loop2400_DTP_472 AS 
(
select 
XML_MD5, 
  ''472'' as date_time_qlfy,
XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET ( DTP_472.value,''svDateFormatQlfr'' ): "$"::VARCHAR AS date_time_frmt_qlfy,
XMLGET ( DTP_472.value,''svDate'' ): "$"::VARCHAR AS service_date
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) DTP_472
where segment = ''Loop2400''
and GET(DTP_472.value, ''@'') = ''DTP_472''
)

,

 Loop2400_Loop2430_CAS1 as (   
    select  
    XML_MD5,  
    XMLGET( Loop2430.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,     
    XMLGET( CAS1.value, ''svAdjGroupCode'' ):"$"::VARCHAR  As svAdjGroupCode,
    SUBSTR(GET(CAS1_A.VALUE, ''@''),  1, LEN(GET(CAS1_A.VALUE, ''@''))-1) AS key,
    GET(CAS1_A.VALUE, ''$'')  as value,
    SUBSTR(GET(CAS1_A.VALUE, ''@''),  -1, 1) AS grp,
    Loop2430.INDEX as Loop2430_IDX
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2430, LATERAL FLATTEN(to_array(GET(Loop2430.value,''$'')))  CAS1, LATERAL FLATTEN(to_array(GET(CAS1.value,''$'')))  CAS1_A
    where segment = ''Loop2400''
    and GET( Loop2430.value, ''@'') = ''Loop2430''
    and GET( CAS1.value, ''@'') = ''CAS1''
    AND CAS1_A.INDEX in (1,2,3,4,5,6,7,8,9,10,11,12)
   ) 



,

 Loop2400_Loop2430_CAS2 as (   
    select  
    XML_MD5,   
    XMLGET( Loop2430.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,     
    XMLGET( CAS2.value, ''svAdjGroupCode'' ):"$"::VARCHAR  As svAdjGroupCode,
    SUBSTR(GET(CAS2_A.VALUE, ''@''),  1, LEN(GET(CAS2_A.VALUE, ''@''))-1) AS key,
    GET(CAS2_A.VALUE, ''$'')  as value,
    SUBSTR(GET(CAS2_A.VALUE, ''@''),  -1, 1) AS grp,
    Loop2430.INDEX as Loop2430_IDX
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2430, LATERAL FLATTEN(to_array(GET(Loop2430.value,''$'')))  CAS2, LATERAL FLATTEN(to_array(GET(CAS2.value,''$'')))  CAS2_A
    where segment = ''Loop2400''
    and GET( Loop2430.value, ''@'') = ''Loop2430''
    and GET( CAS2.value, ''@'') = ''CAS2''
    AND CAS2_A.INDEX in (1,2,3,4,5,6,7,8,9,10,11,12)
   ) 
,

Loop2400_Loop2430_CAS3 AS 
(
    select  
    XML_MD5, 
    XMLGET( Loop2430.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,     
    XMLGET( CAS3.value, ''svAdjGroupCode'' ):"$"::VARCHAR  As svAdjGroupCode,
    SUBSTR(GET(CAS3_A.VALUE, ''@''),  1, LEN(GET(CAS3_A.VALUE, ''@''))-1) AS key,
    GET(CAS3_A.VALUE, ''$'')  as value,
    SUBSTR(GET(CAS3_A.VALUE, ''@''),  -1, 1) AS grp,
    Loop2430.INDEX as Loop2430_IDX
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2430, LATERAL FLATTEN(to_array(GET(Loop2430.value,''$'')))  CAS3, LATERAL FLATTEN(to_array(GET(CAS3.value,''$'')))  CAS3_A
    where segment = ''Loop2400''
    and GET( Loop2430.value, ''@'') = ''Loop2430''
    and GET( CAS3.value, ''@'') = ''CAS3''
    AND CAS3_A.INDEX in (1,2,3,4,5,6,7,8,9,10,11,12)
)

,

Loop2400_Loop2430_CAS4 AS 
(
    select  
    XML_MD5, 
    XMLGET( Loop2430.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,     
    XMLGET( CAS4.value, ''svAdjGroupCode'' ):"$"::VARCHAR  As svAdjGroupCode,
    SUBSTR(GET(CAS4_A.VALUE, ''@''),  1, LEN(GET(CAS4_A.VALUE, ''@''))-1) AS key,
    GET(CAS4_A.VALUE, ''$'')  as value,
    SUBSTR(GET(CAS4_A.VALUE, ''@''),  -1, 1) AS grp,
    Loop2430.INDEX as Loop2430_IDX
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2430, LATERAL FLATTEN(to_array(GET(Loop2430.value,''$'')))  CAS4, LATERAL FLATTEN(to_array(GET(CAS4.value,''$'')))  CAS4_A
    where segment = ''Loop2400''
    and GET( Loop2430.value, ''@'') = ''Loop2430''
    and GET( CAS4.value, ''@'') = ''CAS4''
    AND CAS4_A.INDEX in (1,2,3,4,5,6,7,8,9,10,11,12)
)

,

Loop2400_Loop2430_CAS5 AS 
(
    select  
    XML_MD5, 
    XMLGET( Loop2430.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,     
    XMLGET( CAS5.value, ''svAdjGroupCode'' ):"$"::VARCHAR  As svAdjGroupCode,
    SUBSTR(GET(CAS5_A.VALUE, ''@''),  1, LEN(GET(CAS5_A.VALUE, ''@''))-1) AS key,
    GET(CAS5_A.VALUE, ''$'')  as value,
    SUBSTR(GET(CAS5_A.VALUE, ''@''),  -1, 1) AS grp,
    Loop2430.INDEX as Loop2430_IDX
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2430, LATERAL FLATTEN(to_array(GET(Loop2430.value,''$'')))  CAS5, LATERAL FLATTEN(to_array(GET(CAS5.value,''$'')))  CAS5_A
    where segment = ''Loop2400''
    and GET( Loop2430.value, ''@'') = ''Loop2430''
    and GET( CAS5.value, ''@'') = ''CAS5''
    AND CAS5_A.INDEX in (1,2,3,4,5,6,7,8,9,10,11,12)
)

,

Loop2400_Loop2430 (XML_MD5, SL_SEQ_NUM, CAS_ADJ_GROUP_CODE, CAS_ADJ_REASON_CODE,CAS_ADJ_AMT) AS 

(
  
select *  EXCLUDE (GRP, Loop2430_IDX)  from Loop2400_Loop2430_CAS1
PIVOT( MAX (VALUE) FOR KEY IN (''svAdjReasonCode'',''svAdjAmt''))    
UNION ALL
select *  EXCLUDE (GRP, Loop2430_IDX)  from Loop2400_Loop2430_CAS2
PIVOT( MAX (VALUE) FOR KEY IN (''svAdjReasonCode'',''svAdjAmt'')) 
UNION ALL
select *  EXCLUDE (GRP, Loop2430_IDX)  from Loop2400_Loop2430_CAS3
PIVOT( MAX (VALUE) FOR KEY IN (''svAdjReasonCode'',''svAdjAmt'')) 
UNION ALL
select *  EXCLUDE (GRP, Loop2430_IDX) from Loop2400_Loop2430_CAS4
PIVOT( MAX (VALUE) FOR KEY IN (''svAdjReasonCode'',''svAdjAmt'')) 
UNION ALL
select *  EXCLUDE (GRP, Loop2430_IDX)  from Loop2400_Loop2430_CAS5
PIVOT( MAX (VALUE) FOR KEY IN (''svAdjReasonCode'',''svAdjAmt'')) 

)




,


Loop2300_HI_ABK_ABF AS 
(
select 
XML_MD5, 
COALESCE(CONCAT(''ABK:'',XMLGET ( HI_ABK_ABF.value,''hcCodeInfo1'' ): "$"::VARCHAR), '''') health_care_code_info,

ARRAY_CONSTRUCT (  
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo2'' ): "$"::VARCHAR), '''')  , 
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo3'' ): "$"::VARCHAR), '''')   ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo4'' ): "$"::VARCHAR), '''')   ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo5'' ): "$"::VARCHAR), '''')   ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo6'' ): "$"::VARCHAR), '''')  ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo7'' ): "$"::VARCHAR), '''')   ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo8'' ): "$"::VARCHAR), '''')   ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo9'' ): "$"::VARCHAR), '''')   ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo10'' ): "$"::VARCHAR), '''')  ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo11'' ): "$"::VARCHAR), '''')  ,
                  COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABK_ABF.value,''hcAdditionalCodeInfo12'' ): "$"::VARCHAR), '''')  
               
                ) AS health_care_additional_code_info

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_ABK_ABF
where segment = ''Loop2300''
and GET(HI_ABK_ABF.value, ''@'') = ''HI_ABK_ABF''
)
,

Loop2300_HI_BG AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode1'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode2'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode3'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode4'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode5'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode6'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode7'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode8'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode9'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode10'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode11'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode12'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode13'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode14'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode15'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode16'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode17'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode18'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode19'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode20'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode21'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode22'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode23'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BG:'',XMLGET ( HI_BG.value,''hcConditionCode24'' ): "$"::VARCHAR),'''') 
  ) AS hc_condition_codes


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_BG
where segment = ''Loop2300''
and GET(HI_BG.value, ''@'') = ''HI_BG''
)

,


Loop2300_AMT_F5 AS 
(
select 
XML_MD5,XMLGET ( AMT_F5.value,''patAmtPaid'' ): "$"::VARCHAR AS patient_amt_paid


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) AMT_F5
where segment = ''Loop2300''
and GET(AMT_F5.value, ''@'') = ''AMT_F5''
)


,

Loop2300_REF_4N AS 
(
select 
XML_MD5, XMLGET ( REF_4N.value,''serviceAuthExceptionCode'' ): "$"::VARCHAR AS service_auth_exception_code


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_4N
where segment = ''Loop2300''
and GET(REF_4N.value, ''@'') = ''REF_4N''
)

,

Loop2300_REF_X4 AS 
(
select 
XML_MD5, XMLGET ( REF_X4.value,''clinicalLabAmendmentNumber'' ): "$"::VARCHAR AS clinical_lab_amendment_num
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_X4
where segment = ''Loop2300''
and GET(REF_X4.value, ''@'') = ''REF_X4''
)

,

Loop2300_REF_D9 AS 
(
select 
XML_MD5, XMLGET ( REF_D9.value,''networkTraceNumber'' ): "$"::VARCHAR AS network_trace_number
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_D9
where segment = ''Loop2300''
and GET(REF_D9.value, ''@'') = ''REF_D9''
)


,

Loop2300_REF_EA AS 
(
select 
XML_MD5, XMLGET ( REF_EA.value,''medicalRecordNumber'' ): "$"::VARCHAR AS medical_record_number
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_EA
where segment = ''Loop2300''
and GET(REF_EA.value, ''@'') = ''REF_EA''
)
,

Loop2300_REF_P4 AS 
(
select 
XML_MD5, XMLGET ( REF_P4.value,''demonstrationProjectId'' ): "$"::VARCHAR AS demonstration_project_id


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_P4
where segment = ''Loop2300''
and GET(REF_P4.value, ''@'') = ''REF_P4''
)
,

Loop2300_NTE_ADD AS 
(
select 
XML_MD5, XMLGET ( NTE_ADD.value,''billingNoteText'' ): "$"::VARCHAR AS clm_billing_note_text

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) NTE_ADD
where segment = ''Loop2300''
and GET(NTE_ADD.value, ''@'') = ''NTE_ADD''
)

,


Loop2300_REF_F8 AS 
(
select 
XML_MD5, XMLGET ( REF_F8.value,''payerClmControlNumber'' ): "$"::VARCHAR AS payerClmControlNumber

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_F8
where segment = ''Loop2300''
and GET(REF_F8.value, ''@'') = ''REF_F8''
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


Loop2320_Loop2330B_REF_F8 AS 
(
    select  
    XML_MD5, 
    segments.IDX, 
    XMLGET ( REF_F8.value,''payerClmControlNumber'' ): "$"::VARCHAR AS payer_clm_ctrl_num
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2330B, LATERAL FLATTEN(to_array(GET(Loop2330B.value,''$'')))  REF_F8
    where segment = ''Loop2320''
    and GET( Loop2330B.value, ''@'') = ''Loop2330B''
    and GET( REF_F8.value, ''@'') = ''REF_F8''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY XML_MD5 ORDER BY segments.IDX)  = 1

)
,

Loop2320_MOA_1 AS 
(
select 
XML_MD5, segments.IDX, 
XMLGET ( MOA.value,''reimbursementRate'' ): "$"::VARCHAR AS sv_reimbursement_rate,
XMLGET ( MOA.value,''hcpcsPayableAmt'' ): "$"::VARCHAR AS sv_hcpcs_payable_amt
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) MOA
where segment = ''Loop2320''
and GET(MOA.value, ''@'') = ''MOA''

  
)
,


Loop2320_MOA_2 AS 
(
select XML_MD5, segments.IDX, GET(MOA1.value, ''$'')::VARCHAR as sv_clm_payment_remark_code

from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) MOA , LATERAL FLATTEN(to_array(GET(MOA.value,''$''))) MOA1 
where segment = ''Loop2320''
and GET(MOA.value, ''@'') = ''MOA''
AND SUBSTR(GET(MOA1.value, ''@'')::VARCHAR, 1,  LENGTH(GET(MOA1.value, ''@'')::VARCHAR)-1) = ''clmPaymentRemarkCode''
)

,
Loop2320_MOA AS
(

select Loop2320_Seq.XML_MD5, Loop2320_Seq.IDX, Loop2320_MOA_1.SV_REIMBURSEMENT_RATE, Loop2320_MOA_1.SV_HCPCS_PAYABLE_AMT, Loop2320_MOA_2.SV_CLM_PAYMENT_REMARK_CODE from Loop2320_Seq 
LEFT JOIN Loop2320_MOA_1 ON Loop2320_Seq.XML_MD5 = Loop2320_MOA_1.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_MOA_1.IDX
LEFT JOIN Loop2320_MOA_2 ON Loop2320_Seq.XML_MD5 = Loop2320_MOA_2.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_MOA_2.IDX
WHERE Loop2320_MOA_1.XML_MD5 IS NOT NULL OR Loop2320_MOA_2.XML_MD5 IS NOT NULL 
)

,
Loop2320_AMT_A8 AS 
(
select 
XML_MD5, segments.IDX, 
XMLGET ( AMT_A8.value,''nonCoveredChargeAmt'' ): "$"::VARCHAR AS non_covered_charge_amt
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) AMT_A8
where segment = ''Loop2320''
and GET(AMT_A8.value, ''@'') = ''AMT_A8''
)

,
Loop2320 AS
(
select Loop2320_Seq.XML_MD5, Loop2320_MOA.sv_reimbursement_rate,Loop2320_MOA.sv_hcpcs_payable_amt, Loop2320_MOA.sv_clm_payment_remark_code, Loop2320_AMT_A8.non_covered_charge_amt, 
Loop2320_Loop2330B_REF_F8.payer_clm_ctrl_num 
from Loop2320_Seq 
LEFT JOIN Loop2320_MOA ON Loop2320_Seq.XML_MD5 = Loop2320_MOA.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_MOA.IDX
LEFT JOIN Loop2320_AMT_A8 on Loop2320_Seq.XML_MD5 = Loop2320_AMT_A8.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_AMT_A8.IDX
LEFT JOIN Loop2320_Loop2330B_REF_F8 on Loop2320_Seq.XML_MD5 = Loop2320_Loop2330B_REF_F8.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_Loop2330B_REF_F8.IDX
where (Loop2320_MOA.XML_MD5 is not null or Loop2320_AMT_A8.XML_MD5 is not null or Loop2320_Loop2330B_REF_F8.XML_MD5 is not null)
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
Loop2300_CLM.TOTAL_CLAIM_CHARGE_AMT,
Loop2300_CLM.HEALTHCARESERVICE_LOCATION,
Loop2300_CLM.PROVIDER_ACCEPT_ASSIGN_CODE,
Loop2300_CLM.PROVIDER_BENEFIT_AUTH_CODE,
Loop2300_CLM.PROVIDER_PATINFO_RELEASE_AUTH_CODE,
Loop2300_CLM.DELAY_REASON_CODE,
-------------
Loop2300_HI_ABK_ABF.HEALTH_CARE_CODE_INFO,
----------
Loop2320.sv_reimbursement_rate,
Loop2320.sv_hcpcs_payable_amt,
Loop2320.sv_clm_payment_remark_code,
-----------
 
Loop2400_LX.SL_SEQ_NUM,
---------
REGEXP_REPLACE(
Loop2400_SV1.svProductServiceIdQlfr
||concat('':'', COALESCE(Loop2400_SV1.svProcedureCode,'''')) 
||concat('':'', COALESCE(Loop2400_SV1.svProcedureModifier1,'''')) 
||concat('':'', COALESCE(Loop2400_SV1.svProcedureModifier2,'''')) 
||concat('':'', COALESCE(Loop2400_SV1.svProcedureModifier3,'''')) 
||concat('':'', COALESCE(Loop2400_SV1.svProcedureModifier4,''''))
||concat('':'', COALESCE(Loop2400_SV1.svProcedureDesc,'''')) , ''::+$'', '''') as  PRODUCT_SERVICE_ID_QLFR,

Loop2400_SV1.LINE_ITEM_CHARGE_AMT,
Loop2400_SV1.MEASUREMENT_UNIT,
Loop2400_SV1.SERVICE_UNIT_COUNT,
---------
Loop2400_DTP_472.DATE_TIME_QLFY , 
Loop2400_DTP_472.DATE_TIME_FRMT_QLFY , 
Loop2400_DTP_472.SERVICE_DATE , 
-------------  
Loop2400_Loop2430.CAS_ADJ_GROUP_CODE , 
Loop2400_Loop2430.CAS_ADJ_REASON_CODE , 
Loop2400_Loop2430.CAS_ADJ_AMT , 
--------------- 
CASE WHEN GS.APP_SENDER_CODE = ''APTIX'' THEN ''CH'' 
     WHEN GS.APP_SENDER_CODE = ''COBA'' THEN ''CMS''
     WHEN GS.APP_SENDER_CODE = ''EXELA'' THEN ''EXELA'' END AS VENDOR_CD, 
-------------- 

Loop2300_CLM.PROVIDER_SIGN_INDICATOR , 
Loop2300_CLM.PAT_SIGNATURE_CD , 
------------
Loop2300_CLM.relatedCausesCodeInfo1
||concat('':'', COALESCE(Loop2300_CLM.relatedCausesCodeInfo2,'''')) 
||concat('':'', COALESCE(Loop2300_CLM.stateCode,'''')) 
||concat('':'', COALESCE(Loop2300_CLM.countryCode,'''')) as  RELATED_CAUSE_CODE_INFO,

Loop2300_CLM.specialProgramIndicator AS SPECIAL_PROGRAM_INDICATOR,
-----------
Loop2300_AMT_F5.PATIENT_AMT_PAID , 
------------
Loop2300_REF_4N.SERVICE_AUTH_EXCEPTION_CODE , 
----------
Loop2300_REF_X4.CLINICAL_LAB_AMENDMENT_NUM , 
----------
Loop2300_REF_D9.NETWORK_TRACE_NUMBER , 
--------
Loop2300_REF_EA.MEDICAL_RECORD_NUMBER , 
----------
Loop2300_REF_P4.DEMONSTRATION_PROJECT_ID , 
----------
Loop2300_HI_ABK_ABF.HEALTH_CARE_ADDITIONAL_CODE_INFO , 
----------
Loop2300_HI_BP.anesthesiaProcedureCode as ANESTHESIA_PROCEDURE_CODE,
--------
COALESCE(Loop2300_HI_BG.HC_CONDITION_CODES,array_construct('''', '''','''', '''','''', '''','''', '''','''', '''','''', '''','''', '''','''', '''','''', '''','''', '''','''', '''','''', '''')) AS HC_CONDITION_CODES, 
-------
COALESCE(Loop2300_REF_F8.payerClmControlNumber, Loop2320.PAYER_CLM_CTRL_NUM ) AS PAYER_CLM_CTRL_NUM,
---------
Loop2320.NON_COVERED_CHARGE_AMT , 
----------
CASE WHEN SUBSTR(Loop2400_SV1.DIAGNOSIS_CODE_POINTERS, -1, 1) = '':'' THEN SUBSTR(Loop2400_SV1.DIAGNOSIS_CODE_POINTERS, 1, LENGTH(Loop2400_SV1.DIAGNOSIS_CODE_POINTERS) - 1) ELSE Loop2400_SV1.DIAGNOSIS_CODE_POINTERS END  AS DIAGNOSIS_CODE_POINTERS , 
Loop2400_SV1.EMERGENCY_INDICATOR , 
Loop2400_SV1.FAMILY_PLANNING_INDICATOR , 
Loop2400_SV1.EPSDT_INDICATOR , 
-----------
CLM_BILLING_NOTE_TEXT ,
  
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
     LEFT JOIN Loop2300_CLM ON GS.XML_MD5 = Loop2300_CLM.XML_MD5
     LEFT JOIN Loop2300_HI_BP ON GS.XML_MD5 = Loop2300_HI_BP.XML_MD5
     LEFT JOIN Loop2400_LX  ON GS.XML_MD5 = Loop2400_LX.XML_MD5
     LEFT JOIN Loop2300_HI_ABK_ABF ON GS.XML_MD5 = Loop2300_HI_ABK_ABF.XML_MD5
     LEFT JOIN Loop2300_HI_BG ON GS.XML_MD5 = Loop2300_HI_BG.XML_MD5
     LEFT JOIN Loop2300_AMT_F5 ON GS.XML_MD5 = Loop2300_AMT_F5.XML_MD5
     LEFT JOIN Loop2300_REF_F8 ON GS.XML_MD5 = Loop2300_REF_F8.XML_MD5
     LEFT JOIN Loop2300_REF_4N ON GS.XML_MD5 = Loop2300_REF_4N.XML_MD5
     LEFT JOIN Loop2300_REF_X4 ON GS.XML_MD5 = Loop2300_REF_X4.XML_MD5
     LEFT JOIN Loop2300_REF_D9 ON GS.XML_MD5 = Loop2300_REF_D9.XML_MD5
     LEFT JOIN Loop2300_REF_EA ON GS.XML_MD5 = Loop2300_REF_EA.XML_MD5
     LEFT JOIN Loop2300_REF_P4 ON GS.XML_MD5 = Loop2300_REF_P4.XML_MD5
     LEFT JOIN Loop2300_NTE_ADD ON GS.XML_MD5 = Loop2300_NTE_ADD.XML_MD5
 
     LEFT JOIN Loop2400_SV1 ON Loop2400_LX.XML_MD5 = Loop2400_SV1.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_SV1.sl_seq_num
     LEFT JOIN Loop2400_DTP_472 ON Loop2400_LX.XML_MD5 = Loop2400_DTP_472.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_DTP_472.sl_seq_num
     LEFT JOIN Loop2400_Loop2430 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2430.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_Loop2430.sl_seq_num
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
