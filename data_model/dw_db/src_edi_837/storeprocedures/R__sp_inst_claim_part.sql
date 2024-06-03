USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE "SP_INST_CLAIM_PART"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "FILE_SOURCE" VARCHAR(16777216), "LAST_SUCCESSFUL_LOAD" VARCHAR(16777216), "WH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROGRAM_LIST  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.PROGRAM_LIST'';

V_PROCESS_NAME     VARCHAR := ''EDI_''||UPPER(:FILE_SOURCE)||''_LOADER''; 

V_SUB_PROCESS_NAME VARCHAR DEFAULT ''INST_CLAIM_PART'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_INST_CLAIMS_RAW   VARCHAR := :DB_NAME||''.''||COALESCE(:SRC_EDI_837_SC, ''SRC_EDI_837'')||''.INST_CLAIMS_RAW'';

V_INST_CLAIM_PART   VARCHAR := :DB_NAME||''.''||COALESCE(:TGT_SC, ''SRC_EDI_837'')||''.INST_CLAIM_PART'';



BEGIN

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

   V_STEP := ''STEP1'';
   
   V_STEP_NAME := ''LOAD INST_CLAIM_PART'';
   
   V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 
   
   V_ROWS_PARSED  := (SELECT COUNT(1) FROM IDENTIFIER(:V_INST_CLAIMS_RAW) WHERE FILE_SOURCE = :FILE_SOURCE AND ISDC_LOAD_DT >= :LAST_SUCCESSFUL_LOAD );

INSERT INTO identifier(:V_INST_CLAIM_PART)
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
  TOTAL_CLAIM_CHARGE_AMT , 
  HEALTHCARESERVICE_LOCATION , 
  PROVIDER_ACCEPT_ASSIGN_CODE , 
  PROVIDER_BENEFIT_AUTH_CODE , 
  PROVIDER_PATINFO_RELEASE_AUTH_CODE , 
  DATE_TIME_QLFY , 
  DATE_TIME_FRMT_QLFY , 
  STATEMENT_DATE , 
  ADMIT_TYPE_CODE , 
  ADMIT_SOURCE_CODE , 
  PATIENT_STATUS_CODE , 
  HEALTH_CARE_CODE_INFO , 
  SV_REIMBURSEMENT_RATE , 
  SV_HCPCS_PAYABLE_AMT , 
  SV_CLM_PAYMENT_REMARK_CODE , 
  SL_SEQ_NUM , 
  PRODUCT_SERVICE_ID , 
  PRODUCT_SERVICE_ID_QLFR , 
  LINE_ITEM_CHARGE_AMT , 
  MEASUREMENT_UNIT , 
  SERVICE_UNIT_COUNT , 
  CAS_ADJ_GROUP_CODE , 
  CAS_ADJ_REASON_CODE , 
  CAS_ADJ_AMT , 
  DELAY_REASON_CODE , 
  LINE_ITEM_DENIED_CHARGE_AMT , 
  NETWORK_TRACE_NUMBER , 
  PRINCIPAL_PROCEDURE_INFO , 
  HC_CONDITION_CODES , 
  CLM_LAB_FACILITY_NAME , 
  CLM_LAB_FACILITY_ID , 
  CLM_LAB_FACILITY_ADDR1 , 
  CLM_LAB_FACILITY_ADDR2 , 
  CLM_LAB_FACILITY_CITY , 
  CLM_LAB_FACILITY_STATE , 
  CLM_LAB_FACILITY_ZIP , 
  CLM_LAB_FACILITY_REF_ID_QLFR , 
  CLM_LAB_FACILITY_REF_ID , 
  MEDICAL_RECORD_NUMBER , 
  CLM_NOTE_TEXT , 
  CLM_BILLING_NOTE_TEXT , 
  CLM_ADMITTING_DIAGNOSIS_CD , 
  PATIENT_REASON_FOR_VISIT_CD , 
  EXTERNAL_CAUSE_OF_INJURY , 
  DIAGNOSIS_RELATED_GRP_INFO , 
  OTHER_DIAGNOSIS_CD_INFO , 
  OTHER_PROCEDURE_INFO , 
  OCCURRENCE_SPAN_INFO , 
  OCCURRENCE_INFO , 
  VALUE_INFO , 
  TREATMENT_CD_INFO , 
  OTHER_PAYER_1_PAID_AMT , 
  OTHER_PAYER_2_PAID_AMT , 
  DRUG_PRODUCT_ID_QLFR , 
  DRUG_PRODUCT_ID , 
  DRUG_UNIT_COUNT , 
  DRUG_MEASURE_UNIT , 
  PAYER_CLM_CTRL_NUM , 
  CLM_NOTE_REF_CD ,
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
AND NOT EXISTS ( SELECT 1 FROM  IDENTIFIER(:V_INST_CLAIM_PART) P WHERE P.FILE_SOURCE = :FILE_SOURCE AND R.XML_MD5 = P.XML_MD5)
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
XMLGET ( CLM.value,''patControlNumber'' ): "$"::VARCHAR AS claim_id,
XMLGET ( CLM.value,''clmTotalChargeAmt'' ): "$"::VARCHAR AS total_claim_charge_amt,
XMLGET ( CLM.value,''healthcareServiceLocation'' ): "$"::VARCHAR AS healthcareservice_location,
XMLGET ( CLM.value,''prvAcceptAssignCode'' ): "$"::VARCHAR AS provider_accept_assign_code,
XMLGET ( CLM.value,''prvBenefitAuthCode'' ): "$"::VARCHAR AS provider_benefit_auth_code,
XMLGET ( CLM.value,''prvPatInfoReleaseAuthCode'' ): "$"::VARCHAR AS provider_patinfo_release_auth_code,
XMLGET ( CLM.value,''delayReasonCode'' ): "$"::VARCHAR AS delay_reason_code
from segments , LATERAL FLATTEN(to_array(GET(xml,''$''))) CLM
where segment = ''Loop2300''
and GET(CLM.value, ''@'') = ''CLM''
)
,


Loop2300_REF_F8 AS 
(
select 
XML_MD5, XMLGET ( REF_F8.value,''payerClmControlNumber'' ): "$"::VARCHAR AS payer_clm_ctrl_num
from segments, LATERAL FLATTEN(xml:"$") REF_F8
where segment = ''Loop2300''
and GET(REF_F8.value, ''@'') = ''REF_F8''
)
,
Loop2300_DTP_434 AS 
(
select 
XML_MD5, 
  ''434'' as date_time_qlfy,
XMLGET ( DTP_434.value,''dateFormatQlfr'' ): "$"::VARCHAR AS date_time_frmt_qlfy,
XMLGET ( DTP_434.value,''date'' ): "$"::VARCHAR AS statement_date
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) DTP_434
where segment = ''Loop2300''
and GET(DTP_434.value, ''@'') = ''DTP_434''
)
,

Loop2300_CL1 AS 
(
select 
XML_MD5, 
XMLGET ( CL1.value,''admitTypeCode'' ): "$"::VARCHAR AS admit_type_code,
XMLGET ( CL1.value,''admitSourceCode'' ): "$"::VARCHAR AS admit_source_code,
XMLGET ( CL1.value,''patStatusCode'' ): "$"::VARCHAR AS patient_status_code
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) CL1
where segment = ''Loop2300''
and GET(CL1.value, ''@'') = ''CL1''
)
,

Loop2300_HI_ABK AS 
(
select 
XML_MD5,
COALESCE(CONCAT(''ABK:'',XMLGET ( HI_ABK.value,''hcCodeInfo'' ): "$"::VARCHAR), '''') health_care_code_info
   
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_ABK
where segment = ''Loop2300''
and GET(HI_ABK.value, ''@'') = ''HI_ABK''
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


Loop2400_SV2 AS 
(
select 
XML_MD5, 
XMLGET( THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
XMLGET ( SV2.value,''svProductServiceId''): "$"::VARCHAR as product_service_id,  
  
XMLGET ( SV2.value,''svProductServiceIdQlfr''): "$"::VARCHAR as svProductServiceIdQlfr,  
XMLGET ( SV2.value,''svProcedureCode''): "$"::VARCHAR as svProcedureCode,
XMLGET ( SV2.value,''svProcedureModifier1''): "$"::VARCHAR as svProcedureModifier1, 
XMLGET ( SV2.value,''svProcedureModifier2''): "$"::VARCHAR as svProcedureModifier2, 
XMLGET ( SV2.value,''svProcedureModifier3''): "$"::VARCHAR as svProcedureModifier3, 
XMLGET ( SV2.value,''svProcedureModifier4''): "$"::VARCHAR as svProcedureModifier4, 
XMLGET ( SV2.value,''svProcedureDesc''): "$"::VARCHAR as svProcedureDesc, 

   
XMLGET ( SV2.value,''svLineItemChargeAmt'' ): "$"::VARCHAR AS line_item_charge_amt,
XMLGET ( SV2.value,''svMeasurementUnit'' ): "$"::VARCHAR AS measurement_unit,
XMLGET ( SV2.value,''svUnitCount'' ): "$"::VARCHAR AS service_unit_count,
XMLGET ( SV2.value,''svLineItemDeniedChargeAmt'' ): "$"::VARCHAR AS line_item_denied_charge_amt

from segments S , LATERAL FLATTEN(to_array(GET(xml,''$''))) SV2 
where segment = ''Loop2400''
and GET(SV2.value, ''@'') = ''SV2''
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

select Loop2320_Seq.XML_MD5, Loop2320_MOA_1.SV_REIMBURSEMENT_RATE, Loop2320_MOA_1.SV_HCPCS_PAYABLE_AMT, Loop2320_MOA_2.SV_CLM_PAYMENT_REMARK_CODE from Loop2320_Seq 
LEFT JOIN Loop2320_MOA_1 ON Loop2320_Seq.XML_MD5 = Loop2320_MOA_1.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_MOA_1.IDX
LEFT JOIN Loop2320_MOA_2 ON Loop2320_Seq.XML_MD5 = Loop2320_MOA_2.XML_MD5 AND Loop2320_Seq.IDX = Loop2320_MOA_2.IDX
WHERE Loop2320_MOA_1.XML_MD5 IS NOT NULL OR Loop2320_MOA_2.XML_MD5 IS NOT NULL 
)

,


Loop2320_SBR AS 
(
select 
XML_MD5, segments.IDX, 
XMLGET ( SBR.value,''otherPayerResponsibilityCode'' ): "$"::VARCHAR AS otherPayerResponsibilityCode
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) SBR
where segment = ''Loop2320''
and GET(SBR.value, ''@'') = ''SBR''
)




,
Loop2320_AMT_D AS 
(
select 
XML_MD5, segments.IDX, 
XMLGET ( AMT_D.value,''otherPayerPaidAmt'' ): "$"::VARCHAR AS otherPayerPaidAmt
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) AMT_D
where segment = ''Loop2320''
and GET(AMT_D.value, ''@'') = ''AMT_D''
)


,
Loop2320_AMT_D_1 as
(select Loop2320_AMT_D.XML_MD5, Loop2320_AMT_D.IDX, Loop2320_AMT_D.otherPayerPaidAmt 
 FROM Loop2320_AMT_D JOIN Loop2320_SBR ON Loop2320_SBR.XML_MD5 = Loop2320_AMT_D.XML_MD5 AND Loop2320_SBR.IDX = Loop2320_AMT_D.IDX 
 WHERE Loop2320_SBR.otherPayerResponsibilityCode = ''P''
 AND Loop2320_AMT_D.otherPayerPaidAmt IS NOT NULL
)

,
Loop2320_AMT_D_2 as
(select Loop2320_AMT_D.XML_MD5, Loop2320_AMT_D.IDX, Loop2320_AMT_D.otherPayerPaidAmt 
 FROM Loop2320_AMT_D JOIN Loop2320_SBR ON Loop2320_SBR.XML_MD5 = Loop2320_AMT_D.XML_MD5 AND Loop2320_SBR.IDX = Loop2320_AMT_D.IDX 
 WHERE Loop2320_SBR.otherPayerResponsibilityCode = ''S''
 AND Loop2320_AMT_D.otherPayerPaidAmt IS NOT NULL
)

,


Loop2320_AMT_D_FINAL AS  
(
select 
claimTrackingId.XML_MD5,
Loop2320_AMT_D_1.otherPayerPaidAmt AS OTHER_PAYER_1_PAID_AMT,
Loop2320_AMT_D_2.otherPayerPaidAmt AS OTHER_PAYER_2_PAID_AMT
from claimTrackingId LEFT JOIN Loop2320_AMT_D_1  ON claimTrackingId.XML_MD5 = Loop2320_AMT_D_1.XML_MD5 
                     LEFT JOIN Loop2320_AMT_D_2  ON claimTrackingId.XML_MD5 = Loop2320_AMT_D_2.XML_MD5 
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



Loop2300_REF_D9 AS 
(
select 
XML_MD5, XMLGET ( REF_D9.value,''networkTraceNumber'' ): "$"::VARCHAR AS network_trace_number
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) REF_D9
where segment = ''Loop2300''
and GET(REF_D9.value, ''@'') = ''REF_D9''
)


,



Loop2300_HI_BBR AS 
(
select 
XML_MD5, 
  
  
  CONCAT(''BBR:'',
  XMLGET ( HI_BBR.value,''principalProcedureInfo'' ): "$"::VARCHAR,'':'',
  XMLGET ( HI_BBR.value,''principalProcedureDateQlfr'' ): "$"::VARCHAR, '':'',
  XMLGET ( HI_BBR.value,''principalProcedureDate'' ): "$"::VARCHAR) AS principal_procedure_info
  
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_BBR
where segment = ''Loop2300''
and GET(HI_BBR.value, ''@'') = ''HI_BBR''
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

Loop2300_NTE_ADD AS 
(
select 
XML_MD5, XMLGET ( NTE_ADD.value,''billingNoteText'' ): "$"::VARCHAR AS clm_billing_note_text
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) NTE_ADD
where segment = ''Loop2300''
and GET(NTE_ADD.value, ''@'') = ''NTE_ADD''
)


,

Loop2300_NTE_1 AS 
(
SELECT 
XML_MD5,
SUBSTR(GET(NTE_A.VALUE, ''@''),  1, LEN(GET(NTE_A.VALUE, ''@''))-1) AS key,
SUBSTR(GET(NTE_A.VALUE, ''@''),  -1, 1) AS grp,
GET(NTE_A.VALUE, ''$'')::VARCHAR  as value
FROM SEGMENTS , LATERAL FLATTEN(to_array(GET(xml,''$''))) NTE, LATERAL FLATTEN(to_array(GET(NTE.value,''$''))) NTE_A  
WHERE SEGMENT = ''Loop2300''
and GET(NTE.value, ''@'') = ''NTE''
and NTE_A.INDEX in (0,1,2,3,4,5,6,7,8,9,10)
)
,
Loop2300_NTE1 (XML_MD5,  clm_note_text, clm_note_ref_cd)
as 
(
SELECT *  exclude grp FROM Loop2300_NTE_1
PIVOT( MAX (VALUE) FOR KEY IN (''clmNoteText'', ''clmNoteRefcd''))  
)  
,
Loop2300_NTE2 
as (select XML_MD5, CASE WHEN CLM_NOTE_REF_CD = ''NTE'' THEN CLM_NOTE_TEXT END AS CLM_NOTE_TEXT, CLM_NOTE_REF_CD from Loop2300_NTE1)
,
Loop2300_NTE as 
(SELECT DISTINCT XML_MD5, CLM_NOTE_TEXT, CLM_NOTE_REF_CD FROM Loop2300_NTE2) 

,
Loop2300_HI_ABJ AS 
(
select 
XML_MD5, 
  
  COALESCE(CONCAT(''ABJ:'',XMLGET ( HI_ABJ.value,''admittingDiagnosisCode'' ): "$"::VARCHAR),'''') AS clm_admitting_diagnosis_cd
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_ABJ
where segment = ''Loop2300''
and GET(HI_ABJ.value, ''@'') = ''HI_ABJ''
)
,
Loop2300_HI_DR AS 
(
select 
XML_MD5, COALESCE(CONCAT(''DR:'',XMLGET ( HI_DR.value,''diagnosisRelatedGrpInfo'' ): "$"::VARCHAR),'''') AS diagnosis_related_grp_info
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_DR
where segment = ''Loop2300''
and GET(HI_DR.value, ''@'') = ''HI_DR''
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

Loop2300_HI_APR AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''APR:'',XMLGET ( HI_APR.value,''patReasonForVisitCode1'' ): "$"::VARCHAR),'''') ,
    COALESCE(CONCAT(''APR:'',XMLGET ( HI_APR.value,''patReasonForVisitCode2'' ): "$"::VARCHAR),'''') ,
    COALESCE(CONCAT(''APR:'',XMLGET ( HI_APR.value,''patReasonForVisitCode3'' ): "$"::VARCHAR),'''') 


  ) AS patient_reason_for_visit_cd


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_APR
where segment = ''Loop2300''
and GET(HI_APR.value, ''@'') = ''HI_APR''
)

 

,

Loop2300_HI_ABN AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury1'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury2'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury3'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury4'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury5'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury6'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury7'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury8'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury9'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury10'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury11'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABN:'',XMLGET ( HI_ABN.value,''externalCauseOfInjury12'' ): "$"::VARCHAR),'''')

  ) AS external_cause_of_injury


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_ABN
where segment = ''Loop2300''
and GET(HI_ABN.value, ''@'') = ''HI_ABN''
)

,

Loop2300_HI_ABF AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo1'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo2'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo3'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo4'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo5'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo6'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo7'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo8'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo9'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo10'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo11'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo12'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo13'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo14'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo15'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo16'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo17'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo18'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo19'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo20'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo21'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo22'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo23'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''ABF:'',XMLGET ( HI_ABF.value,''otherDiagnosisCodeInfo24'' ): "$"::VARCHAR),'''') 
  ) AS other_diagnosis_cd_info


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_ABF
where segment = ''Loop2300''
and GET(HI_ABF.value, ''@'') = ''HI_ABF''
)

,


Loop2300_HI_BBQ AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo1'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo2'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo3'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo4'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo5'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo6'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo7'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo8'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo9'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo10'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo11'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo12'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo13'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo14'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo15'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo16'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo17'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo18'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo19'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo20'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo21'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo22'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo23'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BBQ:'',XMLGET ( HI_BBQ.value,''otherProcedureInfo24'' ): "$"::VARCHAR),'''') 
  ) AS other_procedure_info


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_BBQ
where segment = ''Loop2300''
and GET(HI_BBQ.value, ''@'') = ''HI_BBQ''
)


,

Loop2300_HI_BI AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo1'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo2'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo3'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo4'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo5'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo6'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo7'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo8'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo9'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo10'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo11'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo12'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo13'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo14'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo15'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo16'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo17'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo18'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo19'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo20'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo21'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo22'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo23'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BI:'',XMLGET ( HI_BI.value,''occurrenceSpanInfo24'' ): "$"::VARCHAR),'''') 
  ) AS occurrence_span_info


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_BI
where segment = ''Loop2300''
and GET(HI_BI.value, ''@'') = ''HI_BI''
)
,

Loop2300_HI_BH AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo1'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo2'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo3'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo4'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo5'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo6'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo7'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo8'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo9'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo10'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo11'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo12'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo13'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo14'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo15'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo16'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo17'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo18'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo19'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo20'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo21'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo22'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo23'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BH:'',XMLGET ( HI_BH.value,''occurrenceInfo24'' ): "$"::VARCHAR),'''') 
  ) AS occurrence_info


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_BH
where segment = ''Loop2300''
and GET(HI_BH.value, ''@'') = ''HI_BH''
)
,

Loop2300_HI_BE AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo1'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo2'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo3'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo4'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo5'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo6'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo7'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo8'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo9'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo10'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo11'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo12'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo13'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo14'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo15'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo16'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo17'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo18'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo19'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo20'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo21'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo22'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo23'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''BE:'',XMLGET ( HI_BE.value,''valueInfo24'' ): "$"::VARCHAR),'''') 
  ) AS value_info


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_BE
where segment = ''Loop2300''
and GET(HI_BE.value, ''@'') = ''HI_BE''
)

,



Loop2300_HI_TC AS 
(
select 
XML_MD5, 
ARRAY_CONSTRUCT
  (
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo1'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo2'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo3'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo4'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo5'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo6'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo7'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo8'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo9'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo10'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo11'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo12'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo13'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo14'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo15'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo16'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo17'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo18'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo19'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo20'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo21'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo22'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo23'' ): "$"::VARCHAR),''''),
    COALESCE(CONCAT(''TC:'',XMLGET ( HI_TC.value,''treatmentCodeInfo24'' ): "$"::VARCHAR),'''') 
  ) AS treatment_cd_info


from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) HI_TC
where segment = ''Loop2300''
and GET(HI_TC.value, ''@'') = ''HI_TC''
)
,

Loop2300 AS 
(
select 
XML_MD5
from segments
where segment = ''Loop2300''

)
,

Loop2300_Loop2310E_NM1 AS 
(
select 
XML_MD5, 
  ''G2'' as CLM_LAB_FACILITY_REF_ID_QLFR,
XMLGET( NM1.value, ''labFacilityName'' ):"$"::VARCHAR  As    clm_lab_facility_name,
XMLGET( NM1.value, ''labFacilityId'' ):"$"::VARCHAR  As    clm_lab_facility_id

  
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2310E, LATERAL FLATTEN(to_array(GET(Loop2310E.value,''$''))) NM1
where segment = ''Loop2300''
and (GET(Loop2310E.value, ''@'') = ''Loop2310E'' )
and (GET(NM1.value, ''@'') = ''NM1'' )
)



,
 
Loop2300_Loop2310E_N3 AS 
(
select 
XML_MD5, 
  XMLGET( N3.value, ''labFacilityAddress1'' ):"$"::VARCHAR  As clm_lab_facility_addr1,
  XMLGET( N3.value, ''labFacilityAddress2'' ):"$"::VARCHAR  As clm_lab_facility_addr2
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2310E,LATERAL FLATTEN(to_array(GET(Loop2310E.value,''$''))) N3
where segment = ''Loop2300''
and (GET(Loop2310E.value, ''@'') = ''Loop2310E'' )
and (GET(N3.value, ''@'') = ''N3'')
 
)
,
Loop2300_Loop2310E_N4 AS 
(
select 
XML_MD5, 
   XMLGET( N4.value, ''labFacilityCity'' ):"$"::VARCHAR  As   clm_lab_facility_city,
   XMLGET( N4.value , ''labFacilityState'' ):"$"::VARCHAR  As clm_lab_facility_state,
   XMLGET( N4.value , ''labFacilityZip'' ):"$"::VARCHAR  As   clm_lab_facility_zip
from segments, LATERAL FLATTEN(to_array(GET(xml,''$''))) Loop2310E,LATERAL FLATTEN(to_array(GET(Loop2310E.value,''$''))) N4
where segment = ''Loop2300''
and (GET(Loop2310E.value, ''@'') = ''Loop2310E'' )
       and (GET(N4.value, ''@'') = ''N4'' )
)
,

Loop2300_Loop2310E_REF_G2 AS 
(
    select  
    XML_MD5, ''G2'' as refidqlfy,
    GET(REF_G2_A.VALUE, ''$'')::VARCHAR  as clm_lab_facility_ref_id
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2310E , LATERAL FLATTEN(to_array(GET(Loop2310E.value,''$'')))  REF_G2, LATERAL FLATTEN(to_array(GET(REF_G2.value,''$'')))  REF_G2_A
    where segment = ''Loop2300''
    and GET( Loop2310E.value, ''@'') = ''Loop2310E''
    and GET( REF_G2.value, ''@'') in (''REF_G2'')

)
,

Loop2300_Loop2310E AS (

select Loop2300.XML_MD5,
CLM_LAB_FACILITY_REF_ID_QLFR,
clm_lab_facility_name,
clm_lab_facility_id,
clm_lab_facility_addr1,
clm_lab_facility_addr2,
clm_lab_facility_city,
clm_lab_facility_state,
clm_lab_facility_zip,
clm_lab_facility_ref_id
 from Loop2300 LEFT JOIN Loop2300_Loop2310E_NM1 ON Loop2300.XML_MD5 = Loop2300_Loop2310E_NM1.XML_MD5
                       LEFT JOIN Loop2300_Loop2310E_N3  ON Loop2300.XML_MD5 = Loop2300_Loop2310E_N3.XML_MD5
                       LEFT JOIN Loop2300_Loop2310E_N4  ON Loop2300.XML_MD5 = Loop2300_Loop2310E_N4.XML_MD5
                       LEFT JOIN Loop2300_Loop2310E_REF_G2 ON Loop2300.XML_MD5 = Loop2300_Loop2310E_REF_G2.XML_MD5
                       WHERE (Loop2300_Loop2310E_NM1.XML_MD5 IS NOT NULL OR Loop2300_Loop2310E_N3.XML_MD5 IS NOT NULL OR Loop2300_Loop2310E_N4.XML_MD5 IS NOT NULL OR Loop2300_Loop2310E_REF_G2.XML_MD5 IS NOT NULL)

)


,

Loop2400_Loop2410_LIN AS 
(
    select  
    XML_MD5, XMLGET( Loop2410.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( LIN.value,''svDrugProductIdQlfr'' ): "$"::VARCHAR AS drug_product_id_qlfr,
    XMLGET ( LIN.value,''svDrugProductId'' ): "$"::VARCHAR AS drug_product_id
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2410 , LATERAL FLATTEN(to_array(GET(Loop2410.value,''$'')))  LIN
    where segment = ''Loop2400''
    and GET( Loop2410.value, ''@'') = ''Loop2410''
    and GET( LIN.value, ''@'') in (''LIN'')

)


,

Loop2400_Loop2410_CTP AS 
(
    select  
    XML_MD5, XMLGET( Loop2410.THIS[0],''svSeqNumber''):"$"::VARCHAR  As sl_seq_num,
    XMLGET ( CTP.value,''svDrugUnitCount'' ): "$"::VARCHAR AS drug_unit_count,
    XMLGET ( CTP.value,''svDrugMeasureUnit'' ): "$"::VARCHAR AS drug_measure_unit
    FROM segments,   LATERAL FLATTEN(to_array(GET(xml,''$'')))  Loop2410 , LATERAL FLATTEN(to_array(GET(Loop2410.value,''$'')))  CTP
    where segment = ''Loop2400''
    and GET( Loop2410.value, ''@'') = ''Loop2410''
    and GET( CTP.value, ''@'') in (''CTP'')

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


  CLAIM_ID , 
  TOTAL_CLAIM_CHARGE_AMT , 
  HEALTHCARESERVICE_LOCATION , 
  PROVIDER_ACCEPT_ASSIGN_CODE , 
  PROVIDER_BENEFIT_AUTH_CODE , 
  PROVIDER_PATINFO_RELEASE_AUTH_CODE , 
  DATE_TIME_QLFY , 
  DATE_TIME_FRMT_QLFY , 
  STATEMENT_DATE , 
  ADMIT_TYPE_CODE , 
  ADMIT_SOURCE_CODE , 
  PATIENT_STATUS_CODE , 
  HEALTH_CARE_CODE_INFO , 
  SV_REIMBURSEMENT_RATE , 
  SV_HCPCS_PAYABLE_AMT , 
  SV_CLM_PAYMENT_REMARK_CODE , 
  Loop2400_LX.SL_SEQ_NUM , 
  PRODUCT_SERVICE_ID , 
  REGEXP_REPLACE(
  Loop2400_SV2.svProductServiceIdQlfr
|| concat('':'', COALESCE(Loop2400_SV2.svProcedureCode,'''')) 
|| concat('':'', COALESCE(Loop2400_SV2.svProcedureModifier1,'''')) 
|| concat('':'', COALESCE(Loop2400_SV2.svProcedureModifier2,'''')) 
||concat('':'', COALESCE(Loop2400_SV2.svProcedureModifier3,'''')) 
||concat('':'', COALESCE(Loop2400_SV2.svProcedureModifier4,''''))
||concat('':'', COALESCE(Loop2400_SV2.svProcedureDesc,'''')) , ''::+$'', '''')    as  PRODUCT_SERVICE_ID_QLFR,
 
  LINE_ITEM_CHARGE_AMT , 
  MEASUREMENT_UNIT , 
  SERVICE_UNIT_COUNT , 
  CAS_ADJ_GROUP_CODE , 
  CAS_ADJ_REASON_CODE , 
  CAS_ADJ_AMT , 
  DELAY_REASON_CODE , 
  LINE_ITEM_DENIED_CHARGE_AMT , 
  NETWORK_TRACE_NUMBER , 
  PRINCIPAL_PROCEDURE_INFO , 
  HC_CONDITION_CODES  , 
  CLM_LAB_FACILITY_NAME , 
  CLM_LAB_FACILITY_ID , 
  CLM_LAB_FACILITY_ADDR1 , 
  CLM_LAB_FACILITY_ADDR2 , 
  CLM_LAB_FACILITY_CITY , 
  CLM_LAB_FACILITY_STATE , 
  CLM_LAB_FACILITY_ZIP , 
  CLM_LAB_FACILITY_REF_ID_QLFR , 
  CLM_LAB_FACILITY_REF_ID , 
  MEDICAL_RECORD_NUMBER , 
  CLM_NOTE_TEXT , 
  CLM_BILLING_NOTE_TEXT , 
  CLM_ADMITTING_DIAGNOSIS_CD , 
  PATIENT_REASON_FOR_VISIT_CD, 
  EXTERNAL_CAUSE_OF_INJURY , 
  DIAGNOSIS_RELATED_GRP_INFO , 
  OTHER_DIAGNOSIS_CD_INFO , 
  OTHER_PROCEDURE_INFO , 
  OCCURRENCE_SPAN_INFO , 
  OCCURRENCE_INFO, 
  VALUE_INFO , 
  TREATMENT_CD_INFO, 
  OTHER_PAYER_1_PAID_AMT , 
  OTHER_PAYER_2_PAID_AMT , 
  DRUG_PRODUCT_ID_QLFR , 
  DRUG_PRODUCT_ID , 
  DRUG_UNIT_COUNT , 
  DRUG_MEASURE_UNIT , 
  PAYER_CLM_CTRL_NUM , 
  CLM_NOTE_REF_CD ,

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
     LEFT JOIN Loop2300_REF_F8 ON GS.XML_MD5 = Loop2300_REF_F8.XML_MD5

     LEFT JOIN Loop2300_DTP_434 ON GS.XML_MD5 = Loop2300_DTP_434.XML_MD5
     LEFT JOIN Loop2300_CL1 ON GS.XML_MD5 = Loop2300_CL1.XML_MD5
     LEFT JOIN Loop2300_HI_ABK ON GS.XML_MD5 = Loop2300_HI_ABK.XML_MD5
     LEFT JOIN Loop2300_REF_D9 ON GS.XML_MD5 = Loop2300_REF_D9.XML_MD5
     LEFT JOIN Loop2300_HI_BBR ON GS.XML_MD5 = Loop2300_HI_BBR.XML_MD5
     LEFT JOIN Loop2300_HI_BG ON GS.XML_MD5 = Loop2300_HI_BG.XML_MD5
     LEFT JOIN Loop2300_REF_EA ON GS.XML_MD5 = Loop2300_REF_EA.XML_MD5
   
     LEFT JOIN Loop2300_NTE_ADD ON GS.XML_MD5 = Loop2300_NTE_ADD.XML_MD5
     LEFT JOIN Loop2300_NTE ON GS.XML_MD5 = Loop2300_NTE.XML_MD5

     LEFT JOIN Loop2300_HI_ABJ ON GS.XML_MD5 = Loop2300_HI_ABJ.XML_MD5
     LEFT JOIN Loop2300_HI_APR ON GS.XML_MD5 = Loop2300_HI_APR.XML_MD5
     LEFT JOIN Loop2300_HI_ABN ON GS.XML_MD5 = Loop2300_HI_ABN.XML_MD5
     LEFT JOIN Loop2300_HI_DR ON GS.XML_MD5 = Loop2300_HI_DR.XML_MD5
     LEFT JOIN Loop2300_HI_ABF ON GS.XML_MD5 = Loop2300_HI_ABF.XML_MD5
     LEFT JOIN Loop2300_HI_BBQ ON GS.XML_MD5 = Loop2300_HI_BBQ.XML_MD5
     LEFT JOIN Loop2300_HI_BI ON GS.XML_MD5 = Loop2300_HI_BI.XML_MD5
     LEFT JOIN Loop2300_HI_BH ON GS.XML_MD5 = Loop2300_HI_BH.XML_MD5
     LEFT JOIN Loop2300_HI_BE ON GS.XML_MD5 = Loop2300_HI_BE.XML_MD5
     LEFT JOIN Loop2300_HI_TC ON GS.XML_MD5 = Loop2300_HI_TC.XML_MD5
     LEFT JOIN Loop2300_Loop2310E ON GS.XML_MD5 = Loop2300_Loop2310E.XML_MD5
     LEFT JOIN Loop2320_AMT_D_FINAL ON GS.XML_MD5 = Loop2320_AMT_D_FINAL.XML_MD5

     LEFT JOIN Loop2320_MOA ON GS.XML_MD5 = Loop2320_MOA.XML_MD5
     LEFT JOIN Loop2400_LX ON GS.XML_MD5 = Loop2400_LX.XML_MD5
     LEFT JOIN Loop2400_SV2 ON Loop2400_LX.XML_MD5 = Loop2400_SV2.XML_MD5  AND Loop2400_LX.sl_seq_num = Loop2400_SV2.sl_seq_num
     LEFT JOIN Loop2400_Loop2410_LIN ON Loop2400_LX.XML_MD5 = Loop2400_Loop2410_LIN.XML_MD5  AND Loop2400_LX.sl_seq_num = Loop2400_Loop2410_LIN.sl_seq_num
     LEFT JOIN Loop2400_Loop2410_CTP ON Loop2400_LX.XML_MD5 = Loop2400_Loop2410_CTP.XML_MD5  AND Loop2400_LX.sl_seq_num = Loop2400_Loop2410_CTP.sl_seq_num
     LEFT JOIN Loop2400_Loop2430 ON Loop2400_LX.XML_MD5 = Loop2400_Loop2430.XML_MD5 AND Loop2400_LX.sl_seq_num = Loop2400_Loop2430.sl_seq_num


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
