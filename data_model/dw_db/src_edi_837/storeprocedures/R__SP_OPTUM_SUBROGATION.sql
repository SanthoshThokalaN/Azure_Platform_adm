USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE "SP_OPTUM_SUBROGATION"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "SRC_FOX_SC" VARCHAR(16777216), "LZ_ISDW_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';
V_PROCESS_NAME             VARCHAR        default ''OPTUM_SUBROGATION'';
V_SUB_PROCESS_NAME         VARCHAR        default ''OPTUM_SUBROGATION'';
V_STEP                     VARCHAR;
V_STEP_NAME                VARCHAR;
V_START_TIME               VARCHAR;
V_END_TIME                 VARCHAR;
V_ROWS_PARSED              INTEGER; 
V_ROWS_LOADED              INTEGER; 
V_MESSAGE                  VARCHAR;
V_LAST_QUERY_ID            VARCHAR; 
V_STAGE_QUERY              VARCHAR; 
V_TRANSACTSET_CREATE_START_DATE VARCHAR;
V_TRANSACTSET_CREATE_END_DATE VARCHAR;
V_CLAIM_RECEIPT_START_DATE VARCHAR ;
V_CLAIM_RECEIPT_END_DATE VARCHAR ;

----------Source tables----
	v_indv_clm_nbr         	   varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.indv_clm_nbr'';
 	v_compas_tmp         	       varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.compas_tmp'';
    v_indv_mem_chk               varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.indv_mem_chk'';
 	v_F_CLM_BIL_LN_HIST          varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.F_CLM_BIL_LN_HIST'';
    v_BIL_LN_HIST2               varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.BIL_LN_HIST2'';
    v_d_mbr_info         	       varchar     := :DB_NAME || ''.'' || COALESCE(:LZ_ISDW_SC,''LZ_ISDW'') || ''.d_mbr_info'';
    v_ch_view         	       varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_FOX_SC,''SRC_FOX'') || ''.ch_view'';
    
    v_inst_provider              varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_provider'';
    v_inst_claim_part            varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_claim_part'';
    v_inst_subscriber            varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_subscriber'';
    v_inst_clm_sv_dates          varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.inst_clm_sv_dates'';
    
    v_prof_claim_part            varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_claim_part'';
	v_prof_subscriber            varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_subscriber'';
    v_prof_clm_sv_dates          varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_clm_sv_dates'';
    v_prof_provider              varchar     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.prof_provider'';
    
----------Source tables----
    
   ----------Temp tables---- 
    v_COMPAS_OUTPUT              varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.COMPAS_OUTPUT'';
    v_isdw_output_temp           varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.isdw_output_temp'';
	v_db2import_clm_filtered_2   varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.db2import_clm_filtered_2'';
    v_d_mbr_info_new         	   varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.d_mbr_info_new'';
    
    
    v_inst_claim_temp            varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.inst_claim_temp'';
 	v_inst_provider_temp         varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.inst_provider_temp'';
    v_inst_sub_temp              varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.inst_sub_temp'';
 	v_inst_output                varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.inst_output'';
	v_final_output_temp          varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.final_output_temp'';
 	v_final_output       	       varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.final_output'';
    
    
	v_prof_claim_temp            varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.prof_claim_temp'';
	v_prof_provider_temp         varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.prof_provider_temp'';
    v_prof_sub_temp              varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.prof_sub_temp'';
 	v_prof_output                varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.prof_output'';
	v_final_output_prof_temp 	   varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.final_output_prof_temp'';
 	v_final_output_prof          varchar     := :DB_NAME || ''.'' || COALESCE(:TGT_SC,''SRC_EDI_837'') || ''.final_output_prof'';

   ----------Temp tables---- 






BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH;

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_CLAIM_RECEIPT_START_DATE := (Select DATEADD(WEEK,-5,(:V_CURRENT_DATE-60)));
V_CLAIM_RECEIPT_END_DATE :=  (Select DATEADD(WEEK,-5,(:V_CURRENT_DATE)));
 
 
V_TRANSACTSET_CREATE_START_DATE  := (Select TO_VARCHAR(DATEADD(WEEK,-5,(:V_CURRENT_DATE-90))::DATE,''YYYYMMDD'')) ;
V_TRANSACTSET_CREATE_END_DATE  := (Select TO_VARCHAR(DATEADD(WEEK,-5,(:V_CURRENT_DATE+3))::DATE,''YYYYMMDD'')) ;

   v_step := ''step1'';
   
   v_step_name := ''Load COMPAS_OUTPUT'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_COMPAS_OUTPUT) AS 
select distinct
com.DAYTIME_PHONE_NUM,
com.EVENING_PHONE_NUM,
com.INDIVIDUAL_ID,
com.LANGUAGE_PREFERENCE_ID,
com.EMAIL_ADDR,
com.INSURED_PLAN_EFFECTIVE_DATE,
com.INSURED_PLAN_TERMINATION_DATE,
imc.MEDCR_CLM_NBR,
icn.CHK_NBR,
icn.CLM_NUM,
icn.BIL_LN_NUM
from IDENTIFIER(:V_indv_clm_nbr) icn
join IDENTIFIER(:V_compas_tmp) com on icn.individual_id = com.individual_id and
                           icn.INSURED_PLAN_EFFECTIVE_DATE = com.INSURED_PLAN_EFFECTIVE_DATE
left join IDENTIFIER(:V_indv_mem_chk) imc on icn.individual_id = imc.INDV_ID;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_COMPAS_OUTPUT)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   
 v_step := ''step2'';
   
   v_step_name := ''Load isdw_output_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_isdw_output_temp) AS
select distinct
FCBH.PatientID,
FCBH.ReducedAmount,
FCBH.PlaceOfService,
CASE
  WHEN FCBH.ICD_10_IND = ''Y''
          THEN ''10''
        ELSE ''9''
  END as ICDVersion,
FCBH.PatientAccountNumber,
FCBH.ClaimNumber,
FCBH.ClaimSourceSystem,
FCBH.ClaimType,
FCBH.AllowedAmount,
FCBH.BillLineNumber,
FCBH.TreatingPhysicianID,
FCBH.TreatingPhysicianNPI,
FCBH.ServiceType,
FCBH.DeductibleAmount,
FCBH.CoinsuranceAmount,
FCBH.CopayAmount,
FCBH.PaidAmount,
BLH.RX_UNIT_QTY as DrugQuantity,
BLH.RX_SPL_DAY_CT DaysOfSupply,
BLH.NDC_TXT as DrugName,
BLH.RX_STRGTH_NBR as DrugStrength,
BLH.PROC_CD as ProcedureCodeType,
BLH.PROC_MOD1 as ProcedureModifierCode1,
BLH.PROC_MOD2 as ProcedureModifierCode2,
CASE
  WHEN co.DAYTIME_PHONE_NUM is null
          THEN co.EVENING_PHONE_NUM
        ELSE co.DAYTIME_PHONE_NUM
  END as SubscriberPhone,
co.LANGUAGE_PREFERENCE_ID as PatientLanguage,
co.EMAIL_ADDR as SubscriberEmailAddress,
co.INSURED_PLAN_EFFECTIVE_DATE as PatientEffectiveFromDate,
CASE
  WHEN co.INSURED_PLAN_EFFECTIVE_DATE is not null AND co.INSURED_PLAN_TERMINATION_DATE is null
          THEN ''9999-12-31 00:00:00.0''
        ELSE co.INSURED_PLAN_TERMINATION_DATE
  END as PatientEffectiveToDate,
co.MEDCR_CLM_NBR as PatientMedicareID,
icn.CHK_NBR as CheckNumber,
lpad(icn.acct_nbr,11,''0'') as SubscriberID,
FCBH.PaidDate,
FCBH.DateOfServiceStart,
FCBH.DateOfServiceEnd,
FCBH.BilledAmount
from IDENTIFIER(:V_F_CLM_BIL_LN_HIST) FCBH
left join IDENTIFIER(:V_BIL_LN_HIST2) BLH on FCBH.ch_key = BLH.ch_key
left join IDENTIFIER(:V_compas_output) co on FCBH.ClaimNumber = co.CLM_NUM and
                                      FCBH.BillLineNumber = co.BIL_LN_NUM
join IDENTIFIER(:V_indv_clm_nbr) icn on FCBH.ClaimNumber = icn.CLM_NUM and
                                          FCBH.BillLineNumber = icn.BIL_LN_NUM;
										  
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_isdw_output_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   

 v_step := ''step3'';
   
   v_step_name := ''Load db2import_clm_filtered_2'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   										  
										  
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_db2import_clm_filtered_2) AS
SELECT db2.* FROM IDENTIFIER(:V_ch_view) db2 where db2.clm_recept_dt between :V_CLAIM_RECEIPT_START_DATE and :V_CLAIM_RECEIPT_END_DATE;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_db2import_clm_filtered_2)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   




 v_step := ''step4'';
   
   v_step_name := ''Load inst_claim_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_inst_claim_temp) AS
SELECT DISTINCT
clm.app_sender_code,
clm.grp_control_no,
clm.trancactset_cntl_no,
clm.claim_id,
clm.healthcareservice_location,
clm.statement_date,
clm.health_care_code_info,
clm.sl_seq_num,
clm.product_service_id,
clm.product_service_id_qlfr,
clm.line_item_charge_amt,
clm.hc_condition_codes,
clm.network_trace_number,
clm.other_diagnosis_cd_info,
clm.provider_benefit_auth_code,
translate(clm.clm_billing_note_text,''\\\\|'','' '') as clm_billing_note_text,
clm.clm_admitting_diagnosis_cd,
clm.transactset_create_date
FROM IDENTIFIER(:V_inst_claim_part) clm
where clm.transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE;



V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_inst_claim_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   

 v_step := ''step5'';
   
   v_step_name := ''Load inst_provider_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_inst_provider_temp) AS
SELECT DISTINCT
app_sender_code,
grp_control_no,
trancactset_cntl_no,
batch_cntl_no,
provider_tax_code,
provider_name,
provider_id,
provider_address_1,
provider_address_2,
provider_city,
provider_state,
provider_postalcode,
provider_tax_id,
provider_contact_name,
provider_contact_type,
provider_contact_no,
transactset_create_date
FROM IDENTIFIER(:V_inst_provider) clm
where clm.transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_inst_provider_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   

 v_step := ''step6'';
   
   v_step_name := ''Load d_mbr_info_new'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_d_mbr_info_new) AS
select ACCT_NBR,MBR_INFO_SK_STRT_DT,MBR_INFO_SK_END_DT,ETL_LST_BTCH_ID,LST_NM,FST_NM,MIDL_NM,MEDCR_CLM_NBR,GDR_CD,DOB_ID,ADDR_LN_1,ADDR_LN_2,CTY,ST_CD,ZIP_CD,CURR_ACCT_NBR from (select ACCT_NBR,MBR_INFO_SK_STRT_DT,MBR_INFO_SK_END_DT,ETL_LST_BTCH_ID,LST_NM,FST_NM,MIDL_NM,MEDCR_CLM_NBR,GDR_CD,DOB_ID,ADDR_LN_1,ADDR_LN_2,CTY,ST_CD,ZIP_CD,CURR_ACCT_NBR,max(ETL_LST_BTCH_ID) over (partition by CURR_ACCT_NBR) as last_modified from IDENTIFIER(:V_d_mbr_info)) as sub where ETL_LST_BTCH_ID = last_modified;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_d_mbr_info_new)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   
 v_step := ''step7'';
   
   v_step_name := ''Load inst_sub_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_inst_sub_temp) AS
SELECT DISTINCT
sub.app_sender_code,
sub.grp_control_no,
sub.trancactset_cntl_no,
sub.subscriber_id,
mem.LST_NM,
mem.FST_NM,
mem.MIDL_NM,
mem.ADDR_LN_1,
mem.ADDR_LN_2,
mem.CTY,
mem.ST_CD,
mem.ZIP_CD,
mem.GDR_CD,
mem.DOB_ID,
sub.transactset_create_date
FROM IDENTIFIER(:V_inst_subscriber) sub
join IDENTIFIER(:V_d_mbr_info_new) mem on sub.subscriber_id = mem.curr_acct_nbr
where sub.transactset_create_date between  :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE;
  
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_inst_sub_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   
 v_step := ''step8'';
   
   v_step_name := ''Load inst_output'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_inst_output) AS
SELECT distinct
''Fully Insured'' as LineOfBusiness,
''AARP Med Supp'' as Product,
''0'' as ERISAFlag,
''AARP Med Supp'' as EmployerGroupID,
''AARP Med Supp'' as EmployerGroupName,
'''' as EmployerPlanID,
'''' as EmployerPlanName,
mem.subscriber_id as SubscriberID,
'''' as PatientCode,
mem.FST_NM as SubscriberFirstName,
mem.MIDL_NM as SubscriberMiddleName,
mem.LST_NM as SubscriberLastName,
mem.ADDR_LN_1 as SubscriberAddress1,
mem.ADDR_LN_2 as SubscriberAddress2,
mem.CTY as SubscriberCity,
mem.ST_CD as SubscriberState,
mem.ZIP_CD as SubscriberZip,
'''' as SubscriberZipPlus,
''US'' as SubscriberCountry,
'''' as SubscriberSSN,
mem.DOB_ID as SubscriberDOB,
mem.GDR_CD as SubscriberGender,
'''' as SubscriberLanguage,
'''' as SubscriberPhone,
'''' as SubscriberEmailAddress,
mem.subscriber_id as PatientID,
'''' as PatientMedicareID,
'''' as PatientMedicaidID,
''18'' as PatientRelationshipCode,
mem.FST_NM as PatientFirstName,
mem.MIDL_NM as PatientMiddleName,
mem.LST_NM as PatientLastName,
mem.ADDR_LN_1 as PatientAddress1,
mem.ADDR_LN_2 as PatientAddress2,
mem.CTY as PatientCity,
mem.ST_CD as PatientState,
mem.ZIP_CD as PatientZip,
'''' as PatientZipPlus,
''US'' as PatientCountry,
'''' as PatientSSN,
mem.DOB_ID as PatientDOB,
mem.GDR_CD as PatientGender,
'''' as PatientLanguage,
'''' as PatientPhone,
'''' as PatientEmailAddress,
'''' as PatientEffectiveFromDate,
'''' as PatientEffectiveToDate,
'''' as PatientAccountNumber,
prv.provider_id as BillingProviderID,
prv.provider_id as BillingProviderNPI,
prv.provider_tax_id as BillingProviderTaxID,
prv.provider_name as BillingProviderName,
prv.provider_address_1 as BillingProviderAddress1,
prv.provider_address_2 as BillingProviderAddress2,
prv.provider_city as BillingProviderCity,
prv.provider_state as BillingProviderState,
prv.provider_postalcode as BillingProviderZip,
'''' as BillingProviderZipPlus,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderPhone,
CASE
  WHEN prv.provider_contact_type = ''FX''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderFax,
CASE
  WHEN prv.provider_contact_type = ''EM''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderEmailAddress,
prv.provider_contact_name as BillingProviderContactName,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderContactPhone,
'''' as ParticipatingProviderFlag,
'''' as TreatingPhysicianID,
'''' as TreatingPhysicianNPI,
'''' as TreatingPhysicianTIN,
'''' as TreatingPhysicianName,
'''' as ClaimSourceSystem,
db2.clm_num as ucps_clm_num,
clm.sl_seq_num as ClaimLineNumber,
''0'' as AdjustmentIndicator,
'''' as PreviousClaimNumber,
clm.provider_benefit_auth_code as AssignmentIndicator,
'''' as CheckNumber,
'''' as ClaimType,
''I'' as FormType,
'''' as ServiceType,
CONCAT_WS ('''',split(clm.healthcareservice_location,'':'')[0],split(clm.healthcareservice_location,'':'')[2]) as TypeOfBill,
'''' as PlaceOfService,
''0'' as CapitatedClaimFlag,
'''' as PaidDate,
substr(dates.hospitalized_admission_date,1,8) as DateOfServiceStart,
split(clm.statement_date,''-'')[1] as DateOfServiceEnd,
clm.line_item_charge_amt as BilledAmount,
'''' as ReducedAmount,
'''' as DeductibleAmount,
'''' as CoinsuranceAmount,
'''' as CopayAmount,
'''' as AllowedAmount,
'''' as PaidAmount,
'''' as ProcedureCodeType,
split(clm.product_service_id_qlfr,'':'')[1] as ProcedureCode,
split(clm.product_service_id_qlfr,'':'')[2] as ProcedureModifierCode1,
split(clm.product_service_id_qlfr,'':'')[3] as ProcedureModifierCode2,
clm.product_service_id as RevenueCode,
'''' as DrugName,
'''' as DrugType,
'''' as DrugQuantity,
'''' as DrugQuantityQualifier,
'''' as DrugStrength,
'''' as DrugStrengthUnit,
'''' as DaysOfSupply,
'''' as DAWFlag,
'''' as FormularyStatus,
'''' as SIG,
'''' as DosageForm,
'''' as DrugClassCode,
'''' as DEAClassCode,
''0'' as PCEmployment,
'''' as PCAutoAccident,
'''' as PCAutoAccidentState,
'''' as PCOtherAccident,
'''' AccidentDate,
'''' as ICDVersion,
split(clm.health_care_code_info,'':'')[1] as PrimaryDiagCode,
split(clm.clm_admitting_diagnosis_cd,'':'')[1] as AdmittingDiagCode,
trim(split(split(clm.other_diagnosis_cd_info,'','')[0],'':'')[1],''"'') as DiagCode1,
trim(split(split(clm.other_diagnosis_cd_info,'','')[1],'':'')[1],''"'') as DiagCode2,
trim(split(split(clm.other_diagnosis_cd_info,'','')[2],'':'')[1],''"'') as DiagCode3,
trim(split(split(clm.other_diagnosis_cd_info,'','')[3],'':'')[1],''"'') as DiagCode4,
trim(split(split(clm.other_diagnosis_cd_info,'','')[4],'':'')[1],''"'') as DiagCode5,
trim(split(split(clm.other_diagnosis_cd_info,'','')[5],'':'')[1],''"'') as DiagCode6,
trim(split(split(clm.other_diagnosis_cd_info,'','')[6],'':'')[1],''"'') as DiagCode7,
trim(split(split(clm.other_diagnosis_cd_info,'','')[7],'':'')[1],''"'') as DiagCode8,
trim(split(split(clm.other_diagnosis_cd_info,'','')[8],'':'')[1],''"'') as DiagCode9,
trim(split(split(clm.other_diagnosis_cd_info,'','')[9],'':'')[1],''"'') as DiagCode10,
trim(split(split(clm.other_diagnosis_cd_info,'','')[10],'':'')[1],''"'') as DiagCode11,
trim(split(split(clm.other_diagnosis_cd_info,'','')[11],'':'')[1],''"'') as DiagCode12,
trim(split(split(clm.other_diagnosis_cd_info,'','')[12],'':'')[1],''"'') as DiagCode13,
trim(split(split(clm.other_diagnosis_cd_info,'','')[13],'':'')[1],''"'') as DiagCode14,
trim(split(split(clm.other_diagnosis_cd_info,'','')[14],'':'')[1],''"'') as DiagCode15,
trim(split(split(clm.other_diagnosis_cd_info,'','')[15],'':'')[1],''"'') as DiagCode16,
trim(split(split(clm.other_diagnosis_cd_info,'','')[16],'':'')[1],''"'') as DiagCode17,
trim(split(split(clm.other_diagnosis_cd_info,'','')[17],'':'')[1],''"'') as DiagCode18,
trim(split(split(clm.other_diagnosis_cd_info,'','')[18],'':'')[1],''"'') as DiagCode19,
trim(split(split(clm.other_diagnosis_cd_info,'','')[19],'':'')[1],''"'') as DiagCode20,
trim(split(split(clm.other_diagnosis_cd_info,'','')[20],'':'')[1],''"'') as DiagCode21,
trim(split(split(clm.other_diagnosis_cd_info,'','')[21],'':'')[1],''"'') as DiagCode22,
trim(split(split(clm.other_diagnosis_cd_info,'','')[22],'':'')[1],''"'') as DiagCode23,
trim(split(split(clm.other_diagnosis_cd_info,'','')[23],'':'')[1],''"'') as DiagCode24,
'''' as DiagCode25,
'''' as DiagCode26,
'''' as DiagCode27,
'''' as DiagCode28,
'''' as DiagCode29,
'''' as DiagCode30,
''AARP'' as HealthPlanID,
''AARP'' as PayerName,
'''' as PayerState
FROM IDENTIFIER(:V_inst_claim_temp) clm
JOIN IDENTIFIER(:V_inst_sub_temp) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date)
JOIN IDENTIFIER(:V_inst_provider_temp) prv ON (prv.grp_control_no = clm.grp_control_no
                                                 AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND prv.transactset_create_date = clm.transactset_create_date)
JOIN IDENTIFIER(:V_db2import_clm_filtered_2) db2 ON (db2.clh_trk_id IS NOT NULL
                                                AND db2.clh_trk_id = clm.network_trace_number)
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.claim_id,
b.transactset_create_date,
concat_ws('''',b.hospitalized_admission_date) as hospitalized_admission_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.claim_id,
a.transactset_create_date,
LISTAGG(a.group_map[''435'']) as hospitalized_admission_date
FROM (SELECT distinct grp_control_no,
trancactset_cntl_no,
claim_id,
transactset_create_date,
OBJECT_CONSTRUCT(clm_date_type,clm_date) as group_map
FROM IDENTIFIER(:V_inst_clm_sv_dates) where sl_seq_num is null and transactset_create_date BETWEEN :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.claim_id,
a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                               AND concat(dates.trancactset_cntl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.claim_id)
                                               AND dates.transactset_create_date = clm.transactset_create_date)
UNION
SELECT distinct
''Fully Insured'' as LineOfBusiness,
''AARP Med Supp'' as Product,
''0'' as ERISAFlag,
''AARP Med Supp'' as EmployerGroupID,
''AARP Med Supp'' as EmployerGroupName,
'''' as EmployerPlanID,
'''' as EmployerPlanName,
mem.subscriber_id as SubscriberID,
'''' as PatientCode,
mem.FST_NM as SubscriberFirstName,
mem.MIDL_NM as SubscriberMiddleName,
mem.LST_NM as SubscriberLastName,
mem.ADDR_LN_1 as SubscriberAddress1,
mem.ADDR_LN_2 as SubscriberAddress2,
mem.CTY as SubscriberCity,
mem.ST_CD as SubscriberState,
mem.ZIP_CD as SubscriberZip,
'''' as SubscriberZipPlus,
''US'' as SubscriberCountry,
'''' as SubscriberSSN,
mem.DOB_ID as SubscriberDOB,
mem.GDR_CD as SubscriberGender,
'''' as SubscriberLanguage,
'''' as SubscriberPhone,
'''' as SubscriberEmailAddress,
mem.subscriber_id as PatientID,
'''' as PatientMedicareID,
'''' as PatientMedicaidID,
''18'' as PatientRelationshipCode,
mem.FST_NM as PatientFirstName,
mem.MIDL_NM as PatientMiddleName,
mem.LST_NM as PatientLastName,
mem.ADDR_LN_1 as PatientAddress1,
mem.ADDR_LN_2 as PatientAddress2,
mem.CTY as PatientCity,
mem.ST_CD as PatientState,
mem.ZIP_CD as PatientZip,
'''' as PatientZipPlus,
''US'' as PatientCountry,
'''' as PatientSSN,
mem.DOB_ID as PatientDOB,
mem.GDR_CD as PatientGender,
'''' as PatientLanguage,
'''' as PatientPhone,
'''' as PatientEmailAddress,
'''' as PatientEffectiveFromDate,
'''' as PatientEffectiveToDate,
'''' as PatientAccountNumber,
prv.provider_id as BillingProviderID,
prv.provider_id as BillingProviderNPI,
prv.provider_tax_id as BillingProviderTaxID,
prv.provider_name as BillingProviderName,
prv.provider_address_1 as BillingProviderAddress1,
prv.provider_address_2 as BillingProviderAddress2,
prv.provider_city as BillingProviderCity,
prv.provider_state as BillingProviderState,
prv.provider_postalcode as BillingProviderZip,
'''' as BillingProviderZipPlus,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderPhone,
CASE
  WHEN prv.provider_contact_type = ''FX''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderFax,
CASE
  WHEN prv.provider_contact_type = ''EM''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderEmailAddress,
prv.provider_contact_name as BillingProviderContactName,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderContactPhone,
'''' as ParticipatingProviderFlag,
'''' as TreatingPhysicianID,
'''' as TreatingPhysicianNPI,
'''' as TreatingPhysicianTIN,
'''' as TreatingPhysicianName,
'''' as ClaimSourceSystem,
db2.clm_num as ucps_clm_num,
clm.sl_seq_num as ClaimLineNumber,
''0'' as AdjustmentIndicator,
'''' as PreviousClaimNumber,
clm.provider_benefit_auth_code as AssignmentIndicator,
'''' as CheckNumber,
'''' as ClaimType,
''I'' as FormType,
'''' as ServiceType,
CONCAT_WS ('''',split(clm.healthcareservice_location,'':'')[0],split(clm.healthcareservice_location,'':'')[2]) as TypeOfBill,
'''' as PlaceOfService,
''0'' as CapitatedClaimFlag,
'''' as PaidDate,
substr(dates.hospitalized_admission_date,1,8) as DateOfServiceStart,
split(clm.statement_date,''-'')[1] as DateOfServiceEnd,
clm.line_item_charge_amt as BilledAmount,
'''' as ReducedAmount,
'''' as DeductibleAmount,
'''' as CoinsuranceAmount,
'''' as CopayAmount,
'''' as AllowedAmount,
'''' as PaidAmount,
'''' as ProcedureCodeType,
split(clm.product_service_id_qlfr,'':'')[1] as ProcedureCode,
split(clm.product_service_id_qlfr,'':'')[2] as ProcedureModifierCode1,
split(clm.product_service_id_qlfr,'':'')[3] as ProcedureModifierCode2,
clm.product_service_id as RevenueCode,
'''' as DrugName,
'''' as DrugType,
'''' as DrugQuantity,
'''' as DrugQuantityQualifier,
'''' as DrugStrength,
'''' as DrugStrengthUnit,
'''' as DaysOfSupply,
'''' as DAWFlag,
'''' as FormularyStatus,
'''' as SIG,
'''' as DosageForm,
'''' as DrugClassCode,
'''' as DEAClassCode,
''0'' as PCEmployment,
'''' as PCAutoAccident,
'''' as PCAutoAccidentState,
'''' as PCOtherAccident,
'''' AccidentDate,
'''' as ICDVersion,
split(clm.health_care_code_info,'':'')[1] as PrimaryDiagCode,
split(clm.clm_admitting_diagnosis_cd,'':'')[1] as AdmittingDiagCode,
trim(split(split(clm.other_diagnosis_cd_info,'','')[0],'':'')[1],''"'') as DiagCode1,
trim(split(split(clm.other_diagnosis_cd_info,'','')[1],'':'')[1],''"'') as DiagCode2,
trim(split(split(clm.other_diagnosis_cd_info,'','')[2],'':'')[1],''"'') as DiagCode3,
trim(split(split(clm.other_diagnosis_cd_info,'','')[3],'':'')[1],''"'') as DiagCode4,
trim(split(split(clm.other_diagnosis_cd_info,'','')[4],'':'')[1],''"'') as DiagCode5,
trim(split(split(clm.other_diagnosis_cd_info,'','')[5],'':'')[1],''"'') as DiagCode6,
trim(split(split(clm.other_diagnosis_cd_info,'','')[6],'':'')[1],''"'') as DiagCode7,
trim(split(split(clm.other_diagnosis_cd_info,'','')[7],'':'')[1],''"'') as DiagCode8,
trim(split(split(clm.other_diagnosis_cd_info,'','')[8],'':'')[1],''"'') as DiagCode9,
trim(split(split(clm.other_diagnosis_cd_info,'','')[9],'':'')[1],''"'') as DiagCode10,
trim(split(split(clm.other_diagnosis_cd_info,'','')[10],'':'')[1],''"'') as DiagCode11,
trim(split(split(clm.other_diagnosis_cd_info,'','')[11],'':'')[1],''"'') as DiagCode12,
trim(split(split(clm.other_diagnosis_cd_info,'','')[12],'':'')[1],''"'') as DiagCode13,
trim(split(split(clm.other_diagnosis_cd_info,'','')[13],'':'')[1],''"'') as DiagCode14,
trim(split(split(clm.other_diagnosis_cd_info,'','')[14],'':'')[1],''"'') as DiagCode15,
trim(split(split(clm.other_diagnosis_cd_info,'','')[15],'':'')[1],''"'') as DiagCode16,
trim(split(split(clm.other_diagnosis_cd_info,'','')[16],'':'')[1],''"'') as DiagCode17,
trim(split(split(clm.other_diagnosis_cd_info,'','')[17],'':'')[1],''"'') as DiagCode18,
trim(split(split(clm.other_diagnosis_cd_info,'','')[18],'':'')[1],''"'') as DiagCode19,
trim(split(split(clm.other_diagnosis_cd_info,'','')[19],'':'')[1],''"'') as DiagCode20,
trim(split(split(clm.other_diagnosis_cd_info,'','')[20],'':'')[1],''"'') as DiagCode21,
trim(split(split(clm.other_diagnosis_cd_info,'','')[21],'':'')[1],''"'') as DiagCode22,
trim(split(split(clm.other_diagnosis_cd_info,'','')[22],'':'')[1],''"'') as DiagCode23,
trim(split(split(clm.other_diagnosis_cd_info,'','')[23],'':'')[1],''"'') as DiagCode24,
'''' as DiagCode25,
'''' as DiagCode26,
'''' as DiagCode27,
'''' as DiagCode28,
'''' as DiagCode29,
'''' as DiagCode30,
''AARP'' as HealthPlanID,
''AARP'' as PayerName,
'''' as PayerState
FROM IDENTIFIER(:V_inst_claim_temp) clm
JOIN IDENTIFIER(:V_inst_sub_temp) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date)
JOIN IDENTIFIER(:V_inst_provider_temp) prv ON (prv.grp_control_no = clm.grp_control_no
                                                 AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND prv.transactset_create_date = clm.transactset_create_date)
JOIN IDENTIFIER(:V_db2import_clm_filtered_2) db2 ON (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12))
AND (clm.app_sender_code = ''EXELA'')
AND (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL))
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.claim_id,
b.transactset_create_date,
concat_ws('''',b.hospitalized_admission_date) as hospitalized_admission_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.claim_id,
a.transactset_create_date,
LISTAGG(a.group_map[''435'']) as hospitalized_admission_date
FROM (SELECT distinct grp_control_no,
trancactset_cntl_no,
claim_id,
transactset_create_date,
OBJECT_CONSTRUCT(clm_date_type,clm_date) as group_map
FROM IDENTIFIER(:V_inst_clm_sv_dates) where sl_seq_num is null and transactset_create_date BETWEEN  :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.claim_id,
a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                               AND concat(dates.trancactset_cntl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.claim_id)
                                               AND dates.transactset_create_date = clm.transactset_create_date);

											   
											   
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_inst_output)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   

 v_step := ''step9'';
   
   v_step_name := ''Load final_output_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   											   
											   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_final_output_temp) AS
select distinct
pf.LineOfBusiness,
pf.Product,
pf.ERISAFlag,
pf.EmployerGroupID,
pf.EmployerGroupName,
pf.EmployerPlanID,
pf.EmployerPlanName,
pf.SubscriberID,
pf.PatientCode,
pf.SubscriberFirstName,
pf.SubscriberMiddleName,
pf.SubscriberLastName,
pf.SubscriberAddress1,
pf.SubscriberAddress2,
pf.SubscriberCity,
pf.SubscriberState,
pf.SubscriberZip,
pf.SubscriberZipPlus,
pf.SubscriberCountry,
pf.SubscriberSSN,
pf.SubscriberDOB,
pf.SubscriberGender,
pf.SubscriberLanguage,
io.SubscriberPhone,
io.SubscriberEmailAddress,
pf.PatientID,
io.PatientMedicareID,
pf.PatientMedicaidID,
pf.PatientRelationshipCode,
pf.PatientFirstName,
pf.PatientMiddleName,
pf.PatientLastName,
pf.PatientAddress1,
pf.PatientAddress2,
pf.PatientCity,
pf.PatientState,
pf.PatientZip,
pf.PatientZipPlus,
pf.PatientCountry,
pf.PatientSSN,
pf.PatientDOB,
pf.PatientGender,
io.PatientLanguage,
pf.PatientPhone,
pf.PatientEmailAddress,
io.PatientEffectiveFromDate,
io.PatientEffectiveToDate,
io.PatientAccountNumber,
pf.BillingProviderID,
pf.BillingProviderNPI,
pf.BillingProviderTaxID,
pf.BillingProviderName,
pf.BillingProviderAddress1,
pf.BillingProviderAddress2,
pf.BillingProviderCity,
pf.BillingProviderState,
pf.BillingProviderZip,
pf.BillingProviderZipPlus,
max(pf.billingproviderphone) over (partition by pf.ucps_clm_num) as BillingProviderPhone,
max(pf.billingproviderfax) over (partition by pf.ucps_clm_num) as BillingProviderFax,
max(billingprovideremailaddress) over (partition by pf.ucps_clm_num) as BillingProviderEmailAddress,
max(billingprovidercontactname) over (partition by pf.ucps_clm_num) as BillingProviderContactName,
max(pf.billingprovidercontactphone) over (partition by pf.ucps_clm_num) as BillingProviderContactPhone,
pf.ParticipatingProviderFlag,
io.TreatingPhysicianID,
io.TreatingPhysicianNPI,
TreatingPhysicianTIN,
TreatingPhysicianName,
io.ClaimSourceSystem,
io.claimnumber,
io.billlinenumber,
pf.AdjustmentIndicator,
pf.PreviousClaimNumber,
pf.AssignmentIndicator,
io.CheckNumber,
io.ClaimType,
pf.FormType,
io.ServiceType,
pf.TypeOfBill,
io.PlaceOfService,
pf.CapitatedClaimFlag,
io.PaidDate,
io.DateOfServiceStart,
io.DateOfServiceEnd,
io.BilledAmount,
io.ReducedAmount,
io.DeductibleAmount,
io.CoinsuranceAmount,
io.CopayAmount,
io.AllowedAmount,
io.PaidAmount,
io.ProcedureCodeType,
pf.ProcedureCode,
pf.ProcedureModifierCode1,
pf.ProcedureModifierCode2,
pf.RevenueCode,
io.DrugName,
pf.DrugType,
io.DrugQuantity,
pf.DrugQuantityQualifier,
io.DrugStrength,
pf.DrugStrengthUnit,
io.DaysOfSupply,
pf.DAWFlag,
pf.FormularyStatus,
pf.SIG,
pf.DosageForm,
pf.DrugClassCode,
pf.DEAClassCode,
pf.PCEmployment,
pf.PCAutoAccident,
pf.PCAutoAccidentState,
pf.PCOtherAccident,
pf.AccidentDate,
io.ICDVersion,
pf.PrimaryDiagCode,
pf.AdmittingDiagCode,
pf.DiagCode1,
pf.DiagCode2,
pf.DiagCode3,
pf.DiagCode4,
pf.DiagCode5,
pf.DiagCode6,
pf.DiagCode7,
pf.DiagCode8,
pf.DiagCode9,
pf.DiagCode10,
pf.DiagCode11,
pf.DiagCode12,
pf.DiagCode13,
pf.DiagCode14,
pf.DiagCode15,
pf.DiagCode16,
pf.DiagCode17,
pf.DiagCode18,
pf.DiagCode19,
pf.DiagCode20,
pf.DiagCode21,
pf.DiagCode22,
pf.DiagCode23,
pf.DiagCode24,
pf.DiagCode25,
pf.DiagCode26,
pf.DiagCode27,
pf.DiagCode28,
pf.DiagCode29,
pf.DiagCode30,
pf.HealthPlanID,
pf.PayerName,
pf.PayerState
from IDENTIFIER(:V_inst_output) pf
join IDENTIFIER(:V_isdw_output_temp) io on pf.ucps_clm_num = io.ClaimNumber and
pf.ClaimLineNumber = io.BillLineNumber;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_final_output_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   

 v_step := ''step10'';
   
   v_step_name := ''Load final_output'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_final_output) AS
select
LineOfBusiness::STRING AS "LineOfBusiness",
Product::STRING AS "Product",
ERISAFlag::STRING AS "ERISAFlag",
EmployerGroupID::STRING AS "EmployerGroupID",
EmployerGroupName::STRING AS "EmployerGroupName",
EmployerPlanID::STRING AS "EmployerPlanID",
EmployerPlanName::STRING AS "EmployerPlanName",
SubscriberID::STRING AS "SubscriberID",
PatientCode::STRING AS "PatientCode",
SubscriberFirstName::STRING AS "SubscriberFirstName",
SubscriberMiddleName::STRING AS "SubscriberMiddleName",
SubscriberLastName::STRING AS "SubscriberLastName",
SubscriberAddress1::STRING AS "SubscriberAddress1",
SubscriberAddress2::STRING AS "SubscriberAddress2",
SubscriberCity::STRING AS "SubscriberCity",
SubscriberState::STRING AS "SubscriberState",
SubscriberZip::STRING AS "SubscriberZip",
SubscriberZipPlus::STRING AS "SubscriberZipPlus",
SubscriberCountry::STRING AS "SubscriberCountry",
SubscriberSSN::STRING AS "SubscriberSSN",
SubscriberDOB::STRING AS "SubscriberDOB",
SubscriberGender::STRING AS "SubscriberGender",
SubscriberLanguage::STRING AS "SubscriberLanguage",
SubscriberPhone::STRING AS "SubscriberPhone",
SubscriberEmailAddress::STRING AS "SubscriberEmailAddress",
PatientID::STRING AS "PatientID",
PatientMedicareID::STRING AS "PatientMedicareID",
PatientMedicaidID::STRING AS "PatientMedicaidID",
PatientRelationshipCode::STRING AS "PatientRelationshipCode",
PatientFirstName::STRING AS "PatientFirstName",
PatientMiddleName::STRING AS "PatientMiddleName",
PatientLastName::STRING AS "PatientLastName",
PatientAddress1::STRING AS "PatientAddress1",
PatientAddress2::STRING AS "PatientAddress2",
PatientCity::STRING AS "PatientCity",
PatientState::STRING AS "PatientState",
PatientZip::STRING AS "PatientZip",
PatientZipPlus::STRING AS "PatientZipPlus",
PatientCountry::STRING AS "PatientCountry",
PatientSSN::STRING AS "PatientSSN",
PatientDOB::STRING AS "PatientDOB",
PatientGender::STRING AS "PatientGender",
PatientLanguage::STRING AS "PatientLanguage",
PatientPhone::STRING AS "PatientPhone",
PatientEmailAddress::STRING AS "PatientEmailAddress",
PatientEffectiveFromDate::STRING AS "PatientEffectiveFromDate",
PatientEffectiveToDate::STRING AS "PatientEffectiveToDate",
PatientAccountNumber::STRING AS "PatientAccountNumber",
BillingProviderID::STRING AS "BillingProviderID",
BillingProviderNPI::STRING AS "BillingProviderNPI",
BillingProviderTaxID::STRING AS "BillingProviderTaxID",
BillingProviderName::STRING AS "BillingProviderName",
BillingProviderAddress1::STRING AS "BillingProviderAddress1",
BillingProviderAddress2::STRING AS "BillingProviderAddress2",
BillingProviderCity::STRING AS "BillingProviderCity",
BillingProviderState::STRING AS "BillingProviderState",
BillingProviderZip::STRING AS "BillingProviderZip",
BillingProviderZipPlus::STRING AS "BillingProviderZipPlus",
BillingProviderPhone::STRING AS "BillingProviderPhone",
BillingProviderFax::STRING AS "BillingProviderFax",
BillingProviderEmailAddress::STRING AS "BillingProviderEmailAddress",
BillingProviderContactName::STRING AS "BillingProviderContactName",
BillingProviderContactPhone::STRING AS "BillingProviderContactPhone",
ParticipatingProviderFlag::STRING AS "ParticipatingProviderFlag",
TreatingPhysicianID::STRING AS "TreatingPhysicianID",
TreatingPhysicianNPI::STRING AS "TreatingPhysicianNPI",
TreatingPhysicianTIN::STRING AS "TreatingPhysicianTIN",
TreatingPhysicianName::STRING AS "TreatingPhysicianName",
ClaimSourceSystem::STRING AS "ClaimSourceSystem",
ClaimNumber::STRING AS "ClaimNumber",
billlinenumber::STRING AS "ClaimLineNumber",
AdjustmentIndicator::STRING AS "AdjustmentIndicator",
PreviousClaimNumber::STRING AS "PreviousClaimNumber",
AssignmentIndicator::STRING AS "AssignmentIndicator",
CheckNumber::STRING AS "CheckNumber",
ClaimType::STRING AS "ClaimType",
FormType::STRING AS "FormType",
ServiceType::STRING AS "ServiceType",
TypeOfBill::STRING AS "TypeOfBill",
PlaceOfService::STRING AS "PlaceOfService",
CapitatedClaimFlag::STRING AS "CapitatedClaimFlag",
PaidDate::STRING AS "PaidDate",
DateOfServiceStart::STRING AS "DateOfServiceStart",
DateOfServiceEnd::STRING AS "DateOfServiceEnd",
BilledAmount::STRING AS "BilledAmount",
ReducedAmount::STRING AS "ReducedAmount",
DeductibleAmount::STRING AS "DeductibleAmount",
CoinsuranceAmount::STRING AS "CoinsuranceAmount",
CopayAmount::STRING AS "CopayAmount",
AllowedAmount::STRING AS "AllowedAmount",
PaidAmount::STRING AS "PaidAmount",
ProcedureCodeType::STRING AS "ProcedureCodeType",
ProcedureCode::STRING AS "ProcedureCode",
ProcedureModifierCode1::STRING AS "ProcedureModifierCode1",
ProcedureModifierCode2::STRING AS "ProcedureModifierCode2",
RevenueCode::STRING AS "RevenueCode",
DrugName::STRING AS "DrugName",
DrugType::STRING AS "DrugType",
DrugQuantity::STRING AS "DrugQuantity",
DrugQuantityQualifier::STRING AS "DrugQuantityQualifier",
DrugStrength::STRING AS "DrugStrength",
DrugStrengthUnit::STRING AS "DrugStrengthUnit",
DaysOfSupply::STRING AS "DaysOfSupply",
DAWFlag::STRING AS "DAWFlag",
FormularyStatus::STRING AS "FormularyStatus",
SIG::STRING AS "SIG",
DosageForm::STRING AS "DosageForm",
DrugClassCode::STRING AS "DrugClassCode",
DEAClassCode::STRING AS "DEAClassCode",
PCEmployment::STRING AS "PCEmployment",
PCAutoAccident::STRING AS "PCAutoAccident",
PCAutoAccidentState::STRING AS "PCAutoAccidentState",
PCOtherAccident::STRING AS "PCOtherAccident",
AccidentDate::STRING AS "AccidentDate",
ICDVersion::STRING AS "ICDVersion",
PrimaryDiagCode::STRING AS "PrimaryDiagCode",
AdmittingDiagCode::STRING AS "AdmittingDiagCode",
DiagCode1::STRING AS "DiagCode1",
DiagCode2::STRING AS "DiagCode2",
DiagCode3::STRING AS "DiagCode3",
DiagCode4::STRING AS "DiagCode4",
DiagCode5::STRING AS "DiagCode5",
DiagCode6::STRING AS "DiagCode6",
DiagCode7::STRING AS "DiagCode7",
DiagCode8::STRING AS "DiagCode8",
DiagCode9::STRING AS "DiagCode9",
DiagCode10::STRING AS "DiagCode10",
DiagCode11::STRING AS "DiagCode11",
DiagCode12::STRING AS "DiagCode12",
DiagCode13::STRING AS "DiagCode13",
DiagCode14::STRING AS "DiagCode14",
DiagCode15::STRING AS "DiagCode15",
DiagCode16::STRING AS "DiagCode16",
DiagCode17::STRING AS "DiagCode17",
DiagCode18::STRING AS "DiagCode18",
DiagCode19::STRING AS "DiagCode19",
DiagCode20::STRING AS "DiagCode20",
DiagCode21::STRING AS "DiagCode21",
DiagCode22::STRING AS "DiagCode22",
DiagCode23::STRING AS "DiagCode23",
DiagCode24::STRING AS "DiagCode24",
DiagCode25::STRING AS "DiagCode25",
DiagCode26::STRING AS "DiagCode26",
DiagCode27::STRING AS "DiagCode27",
DiagCode28::STRING AS "DiagCode28",
DiagCode29::STRING AS "DiagCode29",
DiagCode30::STRING AS "DiagCode30",
HealthPlanID::STRING AS "HealthPlanID",
PayerName::STRING AS "PayerName",
PayerState::STRING AS "PayerState",
'''' AS "ClientCustomField1",
'''' AS "ClientCustomField2",
'''' AS "ClientCustomField3",
'''' AS "ClientCustomField4",
'''' AS "ClientCustomField5",
'''' AS "ClientCustomField6",
'''' AS "ClientCustomField7",
'''' AS "ClientCustomField8",
'''' AS "ClientCustomField9",
'''' AS "ClientCustomField10"
from IDENTIFIER(:V_final_output_temp);


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_final_output)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL);   

 v_step := ''step11'';
   
   v_step_name := ''Load prof_claim_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_prof_claim_temp) AS
SELECT DISTINCT
clm.app_sender_code,
clm.grp_control_no,
clm.trancactset_cntl_no,
clm.provider_hl_no,
clm.subscriber_hl_no,
clm.claim_id,
clm.healthcareservice_location,
clm.health_care_code_info,
clm.sl_seq_num,
clm.product_service_id_qlfr,
clm.line_item_charge_amt,
clm.vendor_cd,
clm.hc_condition_codes,
clm.network_trace_number,
clm.health_care_additional_code_info,
clm.payer_clm_ctrl_num,
clm.related_cause_code_info,
clm.provider_benefit_auth_code,
clm.clm_billing_note_text,
clm.transactset_create_date,
clm.xml_md5
FROM IDENTIFIER(:V_prof_claim_part) clm
where clm.transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_prof_claim_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL); 

 v_step := ''step12'';
   
   v_step_name := ''Load prof_provider_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_prof_provider_temp) AS
SELECT DISTINCT
app_sender_code,
app_reciever_code,
grp_control_no,
trancactset_cntl_no,
batch_cntl_no,
provider_hl_no,
provider_type,
provider_name,
provider_name_first,
provider_name_middle,
provider_id,
provider_address_1,
provider_address_2,
provider_city,
provider_state,
provider_postalcode,
refer_id_qlfy,
provider_tax_id,
provider_contact_name,
provider_contact_type,
provider_contact_no,
transactset_create_date,
XML_MD5
FROM IDENTIFIER(:V_prof_provider)
where transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE;


V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_prof_provider_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL); 

 v_step := ''step13'';
   
   v_step_name := ''Load prof_sub_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_prof_sub_temp) AS
SELECT DISTINCT
sub.app_sender_code,
sub.grp_control_no,
sub.trancactset_cntl_no,
sub.subscriber_hl_no,
sub.subscriber_id,
mem.LST_NM,
mem.FST_NM,
mem.MIDL_NM,
mem.ADDR_LN_1,
mem.ADDR_LN_2,
mem.CTY,
mem.ST_CD,
mem.ZIP_CD,
mem.GDR_CD,
mem.DOB_ID,
sub.transactset_create_date,
sub.XML_MD5
FROM IDENTIFIER(:V_prof_subscriber) sub
join IDENTIFIER(:V_d_mbr_info_new) mem on sub.subscriber_id = mem.curr_acct_nbr
where sub.transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_prof_sub_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL); 

 v_step := ''step14'';
   
   v_step_name := ''Load temp prof_output'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_prof_output) AS
SELECT
''Fully Insured'' as LineOfBusiness,
''AARP Med Supp'' as Product,
''0'' as ERISAFlag,
''AARP Med Supp'' as EmployerGroupID,
''AARP Med Supp'' as EmployerGroupName,
'''' as EmployerPlanID,
'''' as EmployerPlanName,
mem.subscriber_id as SubscriberID,
'''' as PatientCode,
mem.FST_NM as SubscriberFirstName,
mem.MIDL_NM as SubscriberMiddleName,
mem.LST_NM as SubscriberLastName,
mem.ADDR_LN_1 as SubscriberAddress1,
mem.ADDR_LN_2 as SubscriberAddress2,
mem.CTY as SubscriberCity,
mem.ST_CD as SubscriberState,
mem.ZIP_CD as SubscriberZip,
'''' as SubscriberZipPlus,
''US'' as SubscriberCountry,
'''' as SubscriberSSN,
mem.DOB_ID as SubscriberDOB,
mem.GDR_CD as SubscriberGender,
'''' as SubscriberLanguage,
'''' as SubscriberPhone,
'''' as SubscriberEmailAddress,
mem.subscriber_id as PatientID,
'''' as PatientMedicareID,
'''' as PatientMedicaidID,
''18'' as PatientRelationshipCode,
mem.FST_NM as PatientFirstName,
mem.MIDL_NM as PatientMiddleName,
mem.LST_NM as PatientLastName,
mem.ADDR_LN_1 as PatientAddress1,
mem.ADDR_LN_2 as PatientAddress2,
mem.CTY as PatientCity,
mem.ST_CD as PatientState,
mem.ZIP_CD as PatientZip,
'''' as PatientZipPlus,
''US'' as PatientCountry,
'''' as PatientSSN,
mem.DOB_ID as PatientDOB,
mem.GDR_CD as PatientGender,
'''' as PatientLanguage,
'''' as PatientPhone,
'''' as PatientEmailAddress,
'''' as PatientEffectiveFromDate,
'''' as PatientEffectiveToDate,
'''' as PatientAccountNumber,
prv.provider_id as BillingProviderID,
prv.provider_id as BillingProviderNPI,
prv.provider_tax_id as BillingProviderTaxID,
prv.provider_name as BillingProviderName,
prv.provider_address_1 as BillingProviderAddress1,
prv.provider_address_2 as BillingProviderAddress2,
prv.provider_city as BillingProviderCity,
prv.provider_state as BillingProviderState,
prv.provider_postalcode as BillingProviderZip,
'''' as BillingProviderZipPlus,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderPhone,
CASE
  WHEN prv.provider_contact_type = ''FX''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderFax,
CASE
  WHEN prv.provider_contact_type = ''EM''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderEmailAddress,
prv.provider_contact_name as BillingProviderContactName,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderContactPhone,
'''' as ParticipatingProviderFlag,
'''' as TreatingPhysicianID,
'''' as TreatingPhysicianNPI,
'''' as TreatingPhysicianTIN,
'''' as TreatingPhysicianName,
'''' as ClaimSourceSystem,
db2.clm_num as ucps_clm_num,
clm.sl_seq_num as ClaimLineNumber,
''0'' as AdjustmentIndicator,
'''' as PreviousClaimNumber,
clm.provider_benefit_auth_code as AssignmentIndicator,
'''' as CheckNumber,
'''' as ClaimType,
''P'' as FormType,
'''' as ServiceType,
CONCAT_WS ('''',split(clm.healthcareservice_location,'':'')[0],split(clm.healthcareservice_location,'':'')[2]) as TypeOfBill,
'''' as PlaceOfService,
''0'' as CapitatedClaimFlag,
'''' as PaidDate,
substr(dates.hospitalized_admission_date,1,8) as DateOfServiceStart,
dates.hospitalized_discharge_date as DateOfServiceEnd,
clm.line_item_charge_amt as BilledAmount,
'''' as ReducedAmount,
'''' as DeductibleAmount,
'''' as CoinsuranceAmount,
'''' as CopayAmount,
'''' as AllowedAmount,
'''' as PaidAmount,
'''' as ProcedureCodeType,
split(clm.product_service_id_qlfr,'':'')[1] as ProcedureCode,
split(clm.product_service_id_qlfr,'':'')[2] as ProcedureModifierCode1,
split(clm.product_service_id_qlfr,'':'')[3] as ProcedureModifierCode2,
'''' as RevenueCode,
'''' as DrugName,
'''' as DrugType,
'''' as DrugQuantity,
'''' as DrugQuantityQualifier,
'''' as DrugStrength,
'''' as DrugStrengthUnit,
'''' as DaysOfSupply,
'''' as DAWFlag,
'''' as FormularyStatus,
'''' as SIG,
'''' as DosageForm,
'''' as DrugClassCode,
'''' as DEAClassCode,
''0'' as PCEmployment,
CASE
  WHEN split(clm.related_cause_code_info,'':'')[0] = ''AA''
          THEN ''AA''
        ELSE ''''
  END as PCAutoAccident,
split(clm.related_cause_code_info,'':'')[3] as PCAutoAccidentState,
CASE
  WHEN split(clm.related_cause_code_info,'':'')[0] = ''OA''
          THEN ''OA''
        ELSE ''''
  END as PCOtherAccident,
dates.clm_date as AccidentDate,
'''' as ICDVersion,
split(clm.health_care_code_info,'':'')[1] as PrimaryDiagCode,
'''' as AdmittingDiagCode,
trim(split(split(clm.health_care_additional_code_info,'','')[0],'':'')[1],''"'') as DiagCode1,
trim(split(split(clm.health_care_additional_code_info,'','')[1],'':'')[1],''"'') as DiagCode2,
trim(split(split(clm.health_care_additional_code_info,'','')[2],'':'')[1],''"'') as DiagCode3,
trim(split(split(clm.health_care_additional_code_info,'','')[3],'':'')[1],''"'') as DiagCode4,
trim(split(split(clm.health_care_additional_code_info,'','')[4],'':'')[1],''"'') as DiagCode5,
trim(split(split(clm.health_care_additional_code_info,'','')[5],'':'')[1],''"'') as DiagCode6,
trim(split(split(clm.health_care_additional_code_info,'','')[6],'':'')[1],''"'') as DiagCode7,
trim(split(split(clm.health_care_additional_code_info,'','')[7],'':'')[1],''"'') as DiagCode8,
trim(split(split(clm.health_care_additional_code_info,'','')[8],'':'')[1],''"'') as DiagCode9,
trim(split(split(clm.health_care_additional_code_info,'','')[9],'':'')[1],''"'') as DiagCode10,
trim(split(split(clm.health_care_additional_code_info,'','')[10],'':'')[1],''"'') as DiagCode11,
trim(split(split(clm.health_care_additional_code_info,'','')[11],'':'')[1],''"'') as DiagCode12,
trim(split(split(clm.health_care_additional_code_info,'','')[12],'':'')[1],''"'') as DiagCode13,
trim(split(split(clm.health_care_additional_code_info,'','')[13],'':'')[1],''"'') as DiagCode14,
trim(split(split(clm.health_care_additional_code_info,'','')[14],'':'')[1],''"'') as DiagCode15,
trim(split(split(clm.health_care_additional_code_info,'','')[15],'':'')[1],''"'') as DiagCode16,
trim(split(split(clm.health_care_additional_code_info,'','')[16],'':'')[1],''"'') as DiagCode17,
trim(split(split(clm.health_care_additional_code_info,'','')[17],'':'')[1],''"'') as DiagCode18,
trim(split(split(clm.health_care_additional_code_info,'','')[18],'':'')[1],''"'') as DiagCode19,
trim(split(split(clm.health_care_additional_code_info,'','')[19],'':'')[1],''"'') as DiagCode20,
trim(split(split(clm.health_care_additional_code_info,'','')[20],'':'')[1],''"'') as DiagCode21,
trim(split(split(clm.health_care_additional_code_info,'','')[21],'':'')[1],''"'') as DiagCode22,
trim(split(split(clm.health_care_additional_code_info,'','')[22],'':'')[1],''"'') as DiagCode23,
trim(split(split(clm.health_care_additional_code_info,'','')[23],'':'')[1],''"'') as DiagCode24,
'''' as DiagCode25,
'''' as DiagCode26,
'''' as DiagCode27,
'''' as DiagCode28,
'''' as DiagCode29,
'''' as DiagCode30,
''AARP'' as HealthPlanID,
''AARP'' as PayerName,
'''' as PayerState
FROM IDENTIFIER(:V_prof_claim_temp) clm
JOIN IDENTIFIER(:V_prof_sub_temp) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date
												 AND mem.XML_MD5 = clm.XML_MD5)
JOIN IDENTIFIER(:V_prof_provider_temp) prv   ON (prv.grp_control_no = clm.grp_control_no
                                                 AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND prv.provider_hl_no = clm.provider_hl_no
                                                 AND prv.transactset_create_date = clm.transactset_create_date
												 AND prv.XML_MD5 = clm.XML_MD5)
JOIN IDENTIFIER(:V_db2import_clm_filtered_2) db2 ON (db2.member_id = mem.subscriber_id
                                                AND db2.medicare_clm_cntrl_num IS NOT NULL
                                                AND db2.medicare_clm_cntrl_num = clm.payer_clm_ctrl_num
                                                                                                AND clm.vendor_cd = ''CMS'')
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.provider_hl_no,
b.subscriber_hl_no,
b.payer_hl_no,
b.claim_id,
b.clm_date,
b.transactset_create_date,
concat_ws('''',b.hospitalized_admission_date) as hospitalized_admission_date,
concat_ws('''',b.hospitalized_discharge_date) as hospitalized_discharge_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.clm_date,
a.transactset_create_date,
LISTAGG(a.group_map[''435'']) as hospitalized_admission_date,
LISTAGG(a.group_map[''096'']) as hospitalized_discharge_date
FROM (SELECT distinct grp_control_no,
trancactset_cntl_no,
provider_hl_no,
subscriber_hl_no,
payer_hl_no,
claim_id,
clm_date,
transactset_create_date,
OBJECT_CONSTRUCT(clm_date_type,clm_date) as group_map
FROM IDENTIFIER(:V_prof_clm_sv_dates) where sl_seq_num is null and transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.clm_date,
a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                               AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
                                               AND dates.transactset_create_date = clm.transactset_create_date)                                              
UNION
SELECT
''Fully Insured'' as LineOfBusiness,
''AARP Med Supp'' as Product,
''0'' as ERISAFlag,
''AARP Med Supp'' as EmployerGroupID,
''AARP Med Supp'' as EmployerGroupName,
'''' as EmployerPlanID,
'''' as EmployerPlanName,
mem.subscriber_id as SubscriberID,
'''' as PatientCode,
mem.FST_NM as SubscriberFirstName,
mem.MIDL_NM as SubscriberMiddleName,
mem.LST_NM as SubscriberLastName,
mem.ADDR_LN_1 as SubscriberAddress1,
mem.ADDR_LN_2 as SubscriberAddress2,
mem.CTY as SubscriberCity,
mem.ST_CD as SubscriberState,
mem.ZIP_CD as SubscriberZip,
'''' as SubscriberZipPlus,
''US'' as SubscriberCountry,
'''' as SubscriberSSN,
mem.DOB_ID as SubscriberDOB,
mem.GDR_CD as SubscriberGender,
'''' as SubscriberLanguage,
'''' as SubscriberPhone,
'''' as SubscriberEmailAddress,
mem.subscriber_id as PatientID,
'''' as PatientMedicareID,
'''' as PatientMedicaidID,
''18'' as PatientRelationshipCode,
mem.FST_NM as PatientFirstName,
mem.MIDL_NM as PatientMiddleName,
mem.LST_NM as PatientLastName,
mem.ADDR_LN_1 as PatientAddress1,
mem.ADDR_LN_2 as PatientAddress2,
mem.CTY as PatientCity,
mem.ST_CD as PatientState,
mem.ZIP_CD as PatientZip,
'''' as PatientZipPlus,
''US'' as PatientCountry,
'''' as PatientSSN,
mem.DOB_ID as PatientDOB,
mem.GDR_CD as PatientGender,
'''' as PatientLanguage,
'''' as PatientPhone,
'''' as PatientEmailAddress,
'''' as PatientEffectiveFromDate,
'''' as PatientEffectiveToDate,
'''' as PatientAccountNumber,
prv.provider_id as BillingProviderID,
prv.provider_id as BillingProviderNPI,
prv.provider_tax_id as BillingProviderTaxID,
prv.provider_name as BillingProviderName,
prv.provider_address_1 as BillingProviderAddress1,
prv.provider_address_2 as BillingProviderAddress2,
prv.provider_city as BillingProviderCity,
prv.provider_state as BillingProviderState,
prv.provider_postalcode as BillingProviderZip,
'''' as BillingProviderZipPlus,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderPhone,
CASE
  WHEN prv.provider_contact_type = ''FX''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderFax,
CASE
  WHEN prv.provider_contact_type = ''EM''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderEmailAddress,
prv.provider_contact_name as BillingProviderContactName,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderContactPhone,
'''' as ParticipatingProviderFlag,
'''' as TreatingPhysicianID,
'''' as TreatingPhysicianNPI,
'''' as TreatingPhysicianTIN,
'''' as TreatingPhysicianName,
'''' as ClaimSourceSystem,
db2.clm_num as ucps_clm_num,
clm.sl_seq_num as ClaimLineNumber,
''0'' as AdjustmentIndicator,
'''' as PreviousClaimNumber,
clm.provider_benefit_auth_code as AssignmentIndicator,
'''' as CheckNumber,
'''' as ClaimType,
''P'' as FormType,
'''' as ServiceType,
CONCAT_WS ('''',split(clm.healthcareservice_location,'':'')[0],split(clm.healthcareservice_location,'':'')[2]) as TypeOfBill,
'''' as PlaceOfService,
''0'' as CapitatedClaimFlag,
'''' as PaidDate,
substr(dates.hospitalized_admission_date,1,8) as DateOfServiceStart,
dates.hospitalized_discharge_date as DateOfServiceEnd,
clm.line_item_charge_amt as BilledAmount,
'''' as ReducedAmount,
'''' as DeductibleAmount,
'''' as CoinsuranceAmount,
'''' as CopayAmount,
'''' as AllowedAmount,
'''' as PaidAmount,
'''' as ProcedureCodeType,
split(clm.product_service_id_qlfr,'':'')[1] as ProcedureCode,
split(clm.product_service_id_qlfr,'':'')[2] as ProcedureModifierCode1,
split(clm.product_service_id_qlfr,'':'')[3] as ProcedureModifierCode2,
'''' as RevenueCode,
'''' as DrugName,
'''' as DrugType,
'''' as DrugQuantity,
'''' as DrugQuantityQualifier,
'''' as DrugStrength,
'''' as DrugStrengthUnit,
'''' as DaysOfSupply,
'''' as DAWFlag,
'''' as FormularyStatus,
'''' as SIG,
'''' as DosageForm,
'''' as DrugClassCode,
'''' as DEAClassCode,
''0'' as PCEmployment,
CASE
  WHEN split(clm.related_cause_code_info,'':'')[0] = ''AA''
          THEN ''AA''
        ELSE ''''
  END as PCAutoAccident,
split(clm.related_cause_code_info,'':'')[3] as PCAutoAccidentState,
CASE
  WHEN split(clm.related_cause_code_info,'':'')[0] = ''OA''
          THEN ''OA''
        ELSE ''''
  END as PCOtherAccident,
dates.clm_date as AccidentDate,
'''' as ICDVersion,
split(clm.health_care_code_info,'':'')[1] as PrimaryDiagCode,
'''' as AdmittingDiagCode,
trim(split(split(clm.health_care_additional_code_info,'','')[0],'':'')[1],''"'') as DiagCode1,
trim(split(split(clm.health_care_additional_code_info,'','')[1],'':'')[1],''"'') as DiagCode2,
trim(split(split(clm.health_care_additional_code_info,'','')[2],'':'')[1],''"'') as DiagCode3,
trim(split(split(clm.health_care_additional_code_info,'','')[3],'':'')[1],''"'') as DiagCode4,
trim(split(split(clm.health_care_additional_code_info,'','')[4],'':'')[1],''"'') as DiagCode5,
trim(split(split(clm.health_care_additional_code_info,'','')[5],'':'')[1],''"'') as DiagCode6,
trim(split(split(clm.health_care_additional_code_info,'','')[6],'':'')[1],''"'') as DiagCode7,
trim(split(split(clm.health_care_additional_code_info,'','')[7],'':'')[1],''"'') as DiagCode8,
trim(split(split(clm.health_care_additional_code_info,'','')[8],'':'')[1],''"'') as DiagCode9,
trim(split(split(clm.health_care_additional_code_info,'','')[9],'':'')[1],''"'') as DiagCode10,
trim(split(split(clm.health_care_additional_code_info,'','')[10],'':'')[1],''"'') as DiagCode11,
trim(split(split(clm.health_care_additional_code_info,'','')[11],'':'')[1],''"'') as DiagCode12,
trim(split(split(clm.health_care_additional_code_info,'','')[12],'':'')[1],''"'') as DiagCode13,
trim(split(split(clm.health_care_additional_code_info,'','')[13],'':'')[1],''"'') as DiagCode14,
trim(split(split(clm.health_care_additional_code_info,'','')[14],'':'')[1],''"'') as DiagCode15,
trim(split(split(clm.health_care_additional_code_info,'','')[15],'':'')[1],''"'') as DiagCode16,
trim(split(split(clm.health_care_additional_code_info,'','')[16],'':'')[1],''"'') as DiagCode17,
trim(split(split(clm.health_care_additional_code_info,'','')[17],'':'')[1],''"'') as DiagCode18,
trim(split(split(clm.health_care_additional_code_info,'','')[18],'':'')[1],''"'') as DiagCode19,
trim(split(split(clm.health_care_additional_code_info,'','')[19],'':'')[1],''"'') as DiagCode20,
trim(split(split(clm.health_care_additional_code_info,'','')[20],'':'')[1],''"'') as DiagCode21,
trim(split(split(clm.health_care_additional_code_info,'','')[21],'':'')[1],''"'') as DiagCode22,
trim(split(split(clm.health_care_additional_code_info,'','')[22],'':'')[1],''"'') as DiagCode23,
trim(split(split(clm.health_care_additional_code_info,'','')[23],'':'')[1],''"'') as DiagCode24,
'''' as DiagCode25,
'''' as DiagCode26,
'''' as DiagCode27,
'''' as DiagCode28,
'''' as DiagCode29,
'''' as DiagCode30,
''AARP'' as HealthPlanID,
''AARP'' as PayerName,
'''' as PayerState
FROM IDENTIFIER(:V_prof_claim_temp) clm
JOIN IDENTIFIER(:V_prof_sub_temp) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date
												 AND mem.XML_MD5 = clm.XML_MD5)
JOIN IDENTIFIER(:V_prof_provider_temp) prv   ON (prv.grp_control_no = clm.grp_control_no
                                                 AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND prv.provider_hl_no = clm.provider_hl_no
                                                 AND prv.transactset_create_date = clm.transactset_create_date
												 AND prv.XML_MD5 = clm.XML_MD5)
JOIN IDENTIFIER(:V_db2import_clm_filtered_2) db2 ON (db2.member_id = mem.subscriber_id
                                                AND db2.clh_trk_id IS NOT NULL
                                                AND db2.clh_trk_id = clm.network_trace_number
                                                                                                AND clm.vendor_cd = ''CH'')
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.provider_hl_no,
b.subscriber_hl_no,
b.payer_hl_no,
b.claim_id,
b.clm_date,
b.transactset_create_date,
concat_ws('''',b.hospitalized_admission_date) as hospitalized_admission_date,
concat_ws('''',b.hospitalized_discharge_date) as hospitalized_discharge_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.clm_date,
a.transactset_create_date,
LISTAGG(a.group_map[''435'']) as hospitalized_admission_date,
LISTAGG(a.group_map[''096'']) as hospitalized_discharge_date
FROM (SELECT distinct grp_control_no,
trancactset_cntl_no,
provider_hl_no,
subscriber_hl_no,
payer_hl_no,
claim_id,
clm_date,
transactset_create_date,
OBJECT_CONSTRUCT(clm_date_type,clm_date) as group_map
FROM IDENTIFIER(:V_prof_clm_sv_dates) where sl_seq_num is null and transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.clm_date,
a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                               AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
                                               AND dates.transactset_create_date = clm.transactset_create_date)
UNION
SELECT
''Fully Insured'' as LineOfBusiness,
''AARP Med Supp'' as Product,
''0'' as ERISAFlag,
''AARP Med Supp'' as EmployerGroupID,
''AARP Med Supp'' as EmployerGroupName,
'''' as EmployerPlanID,
'''' as EmployerPlanName,
mem.subscriber_id as SubscriberID,
'''' as PatientCode,
mem.FST_NM as SubscriberFirstName,
mem.MIDL_NM as SubscriberMiddleName,
mem.LST_NM as SubscriberLastName,
mem.ADDR_LN_1 as SubscriberAddress1,
mem.ADDR_LN_2 as SubscriberAddress2,
mem.CTY as SubscriberCity,
mem.ST_CD as SubscriberState,
mem.ZIP_CD as SubscriberZip,
'''' as SubscriberZipPlus,
''US'' as SubscriberCountry,
'''' as SubscriberSSN,
mem.DOB_ID as SubscriberDOB,
mem.GDR_CD as SubscriberGender,
'''' as SubscriberLanguage,
'''' as SubscriberPhone,
'''' as SubscriberEmailAddress,
mem.subscriber_id as PatientID,
'''' as PatientMedicareID,
'''' as PatientMedicaidID,
''18'' as PatientRelationshipCode,
mem.FST_NM as PatientFirstName,
mem.MIDL_NM as PatientMiddleName,
mem.LST_NM as PatientLastName,
mem.ADDR_LN_1 as PatientAddress1,
mem.ADDR_LN_2 as PatientAddress2,
mem.CTY as PatientCity,
mem.ST_CD as PatientState,
mem.ZIP_CD as PatientZip,
'''' as PatientZipPlus,
''US'' as PatientCountry,
'''' as PatientSSN,
mem.DOB_ID as PatientDOB,
mem.GDR_CD as PatientGender,
'''' as PatientLanguage,
'''' as PatientPhone,
'''' as PatientEmailAddress,
'''' as PatientEffectiveFromDate,
'''' as PatientEffectiveToDate,
'''' as PatientAccountNumber,
prv.provider_id as BillingProviderID,
prv.provider_id as BillingProviderNPI,
prv.provider_tax_id as BillingProviderTaxID,
prv.provider_name as BillingProviderName,
prv.provider_address_1 as BillingProviderAddress1,
prv.provider_address_2 as BillingProviderAddress2,
prv.provider_city as BillingProviderCity,
prv.provider_state as BillingProviderState,
prv.provider_postalcode as BillingProviderZip,
'''' as BillingProviderZipPlus,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderPhone,
CASE
  WHEN prv.provider_contact_type = ''FX''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderFax,
CASE
  WHEN prv.provider_contact_type = ''EM''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderEmailAddress,
prv.provider_contact_name as BillingProviderContactName,
CASE
  WHEN prv.provider_contact_type = ''TE''
          THEN prv.provider_contact_no
        ELSE ''''
  END as BillingProviderContactPhone,
'''' as ParticipatingProviderFlag,
'''' as TreatingPhysicianID,
'''' as TreatingPhysicianNPI,
'''' as TreatingPhysicianTIN,
'''' as TreatingPhysicianName,
'''' as ClaimSourceSystem,
db2.clm_num as ucps_clm_num,
clm.sl_seq_num as ClaimLineNumber,
''0'' as AdjustmentIndicator,
'''' as PreviousClaimNumber,
clm.provider_benefit_auth_code as AssignmentIndicator,
'''' as CheckNumber,
'''' as ClaimType,
''P'' as FormType,
'''' as ServiceType,
CONCAT_WS ('''',split(clm.healthcareservice_location,'':'')[0],split(clm.healthcareservice_location,'':'')[2]) as TypeOfBill,
'''' as PlaceOfService,
''0'' as CapitatedClaimFlag,
'''' as PaidDate,
substr(dates.hospitalized_admission_date,1,8) as DateOfServiceStart,
dates.hospitalized_discharge_date as DateOfServiceEnd,
clm.line_item_charge_amt as BilledAmount,
'''' as ReducedAmount,
'''' as DeductibleAmount,
'''' as CoinsuranceAmount,
'''' as CopayAmount,
'''' as AllowedAmount,
'''' as PaidAmount,
'''' as ProcedureCodeType,
split(clm.product_service_id_qlfr,'':'')[1] as ProcedureCode,
split(clm.product_service_id_qlfr,'':'')[2] as ProcedureModifierCode1,
split(clm.product_service_id_qlfr,'':'')[3] as ProcedureModifierCode2,
'''' as RevenueCode,
'''' as DrugName,
'''' as DrugType,
'''' as DrugQuantity,
'''' as DrugQuantityQualifier,
'''' as DrugStrength,
'''' as DrugStrengthUnit,
'''' as DaysOfSupply,
'''' as DAWFlag,
'''' as FormularyStatus,
'''' as SIG,
'''' as DosageForm,
'''' as DrugClassCode,
'''' as DEAClassCode,
''0'' as PCEmployment,
CASE
  WHEN split(clm.related_cause_code_info,'':'')[0] = ''AA''
          THEN ''AA''
        ELSE ''''
  END as PCAutoAccident,
split(clm.related_cause_code_info,'':'')[3] as PCAutoAccidentState,
CASE
  WHEN split(clm.related_cause_code_info,'':'')[0] = ''OA''
          THEN ''OA''
        ELSE ''''
  END as PCOtherAccident,
dates.clm_date as AccidentDate,
'''' as ICDVersion,
split(clm.health_care_code_info,'':'')[1] as PrimaryDiagCode,
'''' as AdmittingDiagCode,
trim(split(split(clm.health_care_additional_code_info,'','')[0],'':'')[1],''"'') as DiagCode1,
trim(split(split(clm.health_care_additional_code_info,'','')[1],'':'')[1],''"'') as DiagCode2,
trim(split(split(clm.health_care_additional_code_info,'','')[2],'':'')[1],''"'') as DiagCode3,
trim(split(split(clm.health_care_additional_code_info,'','')[3],'':'')[1],''"'') as DiagCode4,
trim(split(split(clm.health_care_additional_code_info,'','')[4],'':'')[1],''"'') as DiagCode5,
trim(split(split(clm.health_care_additional_code_info,'','')[5],'':'')[1],''"'') as DiagCode6,
trim(split(split(clm.health_care_additional_code_info,'','')[6],'':'')[1],''"'') as DiagCode7,
trim(split(split(clm.health_care_additional_code_info,'','')[7],'':'')[1],''"'') as DiagCode8,
trim(split(split(clm.health_care_additional_code_info,'','')[8],'':'')[1],''"'') as DiagCode9,
trim(split(split(clm.health_care_additional_code_info,'','')[9],'':'')[1],''"'') as DiagCode10,
trim(split(split(clm.health_care_additional_code_info,'','')[10],'':'')[1],''"'') as DiagCode11,
trim(split(split(clm.health_care_additional_code_info,'','')[11],'':'')[1],''"'') as DiagCode12,
trim(split(split(clm.health_care_additional_code_info,'','')[12],'':'')[1],''"'') as DiagCode13,
trim(split(split(clm.health_care_additional_code_info,'','')[13],'':'')[1],''"'') as DiagCode14,
trim(split(split(clm.health_care_additional_code_info,'','')[14],'':'')[1],''"'') as DiagCode15,
trim(split(split(clm.health_care_additional_code_info,'','')[15],'':'')[1],''"'') as DiagCode16,
trim(split(split(clm.health_care_additional_code_info,'','')[16],'':'')[1],''"'') as DiagCode17,
trim(split(split(clm.health_care_additional_code_info,'','')[17],'':'')[1],''"'') as DiagCode18,
trim(split(split(clm.health_care_additional_code_info,'','')[18],'':'')[1],''"'') as DiagCode19,
trim(split(split(clm.health_care_additional_code_info,'','')[19],'':'')[1],''"'') as DiagCode20,
trim(split(split(clm.health_care_additional_code_info,'','')[20],'':'')[1],''"'') as DiagCode21,
trim(split(split(clm.health_care_additional_code_info,'','')[21],'':'')[1],''"'') as DiagCode22,
trim(split(split(clm.health_care_additional_code_info,'','')[22],'':'')[1],''"'') as DiagCode23,
trim(split(split(clm.health_care_additional_code_info,'','')[23],'':'')[1],''"'') as DiagCode24,
'''' as DiagCode25,
'''' as DiagCode26,
'''' as DiagCode27,
'''' as DiagCode28,
'''' as DiagCode29,
'''' as DiagCode30,
''AARP'' as HealthPlanID,
''AARP'' as PayerName,
'''' as PayerState
FROM IDENTIFIER(:V_prof_claim_temp) clm
JOIN IDENTIFIER(:V_prof_sub_temp) mem ON (mem.grp_control_no = clm.grp_control_no
                                                 AND mem.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND mem.subscriber_hl_no = clm.subscriber_hl_no
                                                 AND mem.transactset_create_date = clm.transactset_create_date
												 AND mem.XML_MD5 = clm.XML_MD5)
JOIN IDENTIFIER(:V_prof_provider_temp) prv   ON (prv.grp_control_no = clm.grp_control_no
                                                 AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
                                                 AND prv.provider_hl_no = clm.provider_hl_no
                                                 AND prv.transactset_create_date = clm.transactset_create_date
												 AND prv.XML_MD5 = clm.XML_MD5)
JOIN IDENTIFIER(:V_db2import_clm_filtered_2) db2 ON (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12))
AND (clm.app_sender_code = ''EXELA'')
AND (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL))
LEFT OUTER JOIN (SELECT b.grp_control_no,
b.trancactset_cntl_no,
b.provider_hl_no,
b.subscriber_hl_no,
b.payer_hl_no,
b.claim_id,
b.clm_date,
b.transactset_create_date,
concat_ws('''',b.hospitalized_admission_date) as hospitalized_admission_date,
concat_ws('''',b.hospitalized_discharge_date) as hospitalized_discharge_date
FROM
(SELECT a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.clm_date,
a.transactset_create_date,
LISTAGG(a.group_map[''435'']) as hospitalized_admission_date,
LISTAGG(a.group_map[''096'']) as hospitalized_discharge_date
FROM (SELECT distinct grp_control_no,
trancactset_cntl_no,
provider_hl_no,
subscriber_hl_no,
payer_hl_no,
claim_id,
clm_date,
transactset_create_date,
OBJECT_CONSTRUCT(clm_date_type,clm_date) as group_map
FROM IDENTIFIER(:V_prof_clm_sv_dates) where sl_seq_num is null and transactset_create_date between :V_TRANSACTSET_CREATE_START_DATE and :V_TRANSACTSET_CREATE_END_DATE) a
group by a.grp_control_no,
a.trancactset_cntl_no,
a.provider_hl_no,
a.subscriber_hl_no,
a.payer_hl_no,
a.claim_id,
a.clm_date,
a.transactset_create_date) b) dates ON (dates.grp_control_no = clm.grp_control_no
                                               AND concat(dates.trancactset_cntl_no,''$'',dates.provider_hl_no,''$'',dates.subscriber_hl_no,''$'',dates.claim_id) = concat(clm.trancactset_cntl_no,''$'',clm.provider_hl_no,''$'',clm.subscriber_hl_no,''$'',clm.claim_id)
                                               AND dates.transactset_create_date = clm.transactset_create_date);
											   
V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_prof_output)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL); 

 v_step := ''step15'';
   
   v_step_name := ''Load final_output_prof_temp'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   											   
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_final_output_prof_temp) AS
select distinct
pf.LineOfBusiness,
pf.Product,
pf.ERISAFlag,
pf.EmployerGroupID,
pf.EmployerGroupName,
pf.EmployerPlanID,
pf.EmployerPlanName,
pf.SubscriberID,
pf.PatientCode,
pf.SubscriberFirstName,
pf.SubscriberMiddleName,
pf.SubscriberLastName,
pf.SubscriberAddress1,
pf.SubscriberAddress2,
pf.SubscriberCity,
pf.SubscriberState,
pf.SubscriberZip,
pf.SubscriberZipPlus,
pf.SubscriberCountry,
pf.SubscriberSSN,
pf.SubscriberDOB,
pf.SubscriberGender,
pf.SubscriberLanguage,
io.SubscriberPhone,
io.SubscriberEmailAddress,
pf.PatientID,
io.PatientMedicareID,
pf.PatientMedicaidID,
pf.PatientRelationshipCode,
pf.PatientFirstName,
pf.PatientMiddleName,
pf.PatientLastName,
pf.PatientAddress1,
pf.PatientAddress2,
pf.PatientCity,
pf.PatientState,
pf.PatientZip,
pf.PatientZipPlus,
pf.PatientCountry,
pf.PatientSSN,
pf.PatientDOB,
pf.PatientGender,
io.PatientLanguage,
pf.PatientPhone,
pf.PatientEmailAddress,
io.PatientEffectiveFromDate,
io.PatientEffectiveToDate,
io.PatientAccountNumber,
pf.BillingProviderID,
pf.BillingProviderNPI,
pf.BillingProviderTaxID,
pf.BillingProviderName,
pf.BillingProviderAddress1,
pf.BillingProviderAddress2,
pf.BillingProviderCity,
pf.BillingProviderState,
pf.BillingProviderZip,
pf.BillingProviderZipPlus,
max(pf.billingproviderphone) over (partition by pf.ucps_clm_num) as BillingProviderPhone,
max(pf.billingproviderfax) over (partition by pf.ucps_clm_num) as BillingProviderFax,
max(pf.billingprovideremailaddress) over (partition by pf.ucps_clm_num) as BillingProviderEmailAddress,
max(pf.billingprovidercontactname) over (partition by pf.ucps_clm_num) as BillingProviderContactName,
max(pf.billingprovidercontactphone) over (partition by pf.ucps_clm_num) as BillingProviderContactPhone,
pf.ParticipatingProviderFlag,
io.TreatingPhysicianID,
io.TreatingPhysicianNPI,
TreatingPhysicianTIN,
TreatingPhysicianName,
io.ClaimSourceSystem,
io.claimnumber,
io.billlinenumber,
pf.AdjustmentIndicator,
pf.PreviousClaimNumber,
pf.AssignmentIndicator,
io.CheckNumber,
io.ClaimType,
pf.FormType,
io.ServiceType,
pf.TypeOfBill,
io.PlaceOfService,
pf.CapitatedClaimFlag,
io.PaidDate,
io.DateOfServiceStart,
io.DateOfServiceEnd,
io.BilledAmount,
io.ReducedAmount,
io.DeductibleAmount,
io.CoinsuranceAmount,
io.CopayAmount,
io.AllowedAmount,
io.PaidAmount,
io.ProcedureCodeType,
pf.ProcedureCode,
pf.ProcedureModifierCode1,
pf.ProcedureModifierCode2,
pf.RevenueCode,
io.DrugName,
pf.DrugType,
io.DrugQuantity,
pf.DrugQuantityQualifier,
io.DrugStrength,
pf.DrugStrengthUnit,
io.DaysOfSupply,
pf.DAWFlag,
pf.FormularyStatus,
pf.SIG,
pf.DosageForm,
pf.DrugClassCode,
pf.DEAClassCode,
pf.PCEmployment,
pf.PCAutoAccident,
pf.PCAutoAccidentState,
pf.PCOtherAccident,
max(pf.AccidentDate) over (partition by pf.ucps_clm_num) as AccidentDate,
io.ICDVersion,
pf.PrimaryDiagCode,
pf.AdmittingDiagCode,
pf.DiagCode1,
pf.DiagCode2,
pf.DiagCode3,
pf.DiagCode4,
pf.DiagCode5,
pf.DiagCode6,
pf.DiagCode7,
pf.DiagCode8,
pf.DiagCode9,
pf.DiagCode10,
pf.DiagCode11,
pf.DiagCode12,
pf.DiagCode13,
pf.DiagCode14,
pf.DiagCode15,
pf.DiagCode16,
pf.DiagCode17,
pf.DiagCode18,
pf.DiagCode19,
pf.DiagCode20,
pf.DiagCode21,
pf.DiagCode22,
pf.DiagCode23,
pf.DiagCode24,
pf.DiagCode25,
pf.DiagCode26,
pf.DiagCode27,
pf.DiagCode28,
pf.DiagCode29,
pf.DiagCode30,
pf.HealthPlanID,
pf.PayerName,
pf.PayerState
from IDENTIFIER(:V_prof_output) pf
join IDENTIFIER(:V_isdw_output_temp) io on pf.ucps_clm_num = io.ClaimNumber and
pf.ClaimLineNumber = io.BillLineNumber;

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_final_output_prof_temp)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL); 

 v_step := ''step16'';
   
   v_step_name := ''Load final_output_prof'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:V_final_output_prof) AS
select distinct
LineOfBusiness::STRING AS "LineOfBusiness",
Product::STRING AS "Product",
ERISAFlag::STRING AS "ERISAFlag",
EmployerGroupID::STRING AS "EmployerGroupID",
EmployerGroupName::STRING AS "EmployerGroupName",
EmployerPlanID::STRING AS "EmployerPlanID",
EmployerPlanName::STRING AS "EmployerPlanName",
SubscriberID::STRING AS "SubscriberID",
PatientCode::STRING AS "PatientCode",
SubscriberFirstName::STRING AS "SubscriberFirstName",
SubscriberMiddleName::STRING AS "SubscriberMiddleName",
SubscriberLastName::STRING AS "SubscriberLastName",
SubscriberAddress1::STRING AS "SubscriberAddress1",
SubscriberAddress2::STRING AS "SubscriberAddress2",
SubscriberCity::STRING AS "SubscriberCity",
SubscriberState::STRING AS "SubscriberState",
SubscriberZip::STRING AS "SubscriberZip",
SubscriberZipPlus::STRING AS "SubscriberZipPlus",
SubscriberCountry::STRING AS "SubscriberCountry",
SubscriberSSN::STRING AS "SubscriberSSN",
SubscriberDOB::STRING AS "SubscriberDOB",
SubscriberGender::STRING AS "SubscriberGender",
SubscriberLanguage::STRING AS "SubscriberLanguage",
SubscriberPhone::STRING AS "SubscriberPhone",
SubscriberEmailAddress::STRING AS "SubscriberEmailAddress",
PatientID::STRING AS "PatientID",
PatientMedicareID::STRING AS "PatientMedicareID",
PatientMedicaidID::STRING AS "PatientMedicaidID",
PatientRelationshipCode::STRING AS "PatientRelationshipCode",
PatientFirstName::STRING AS "PatientFirstName",
PatientMiddleName::STRING AS "PatientMiddleName",
PatientLastName::STRING AS "PatientLastName",
PatientAddress1::STRING AS "PatientAddress1",
PatientAddress2::STRING AS "PatientAddress2",
PatientCity::STRING AS "PatientCity",
PatientState::STRING AS "PatientState",
PatientZip::STRING AS "PatientZip",
PatientZipPlus::STRING AS "PatientZipPlus",
PatientCountry::STRING AS "PatientCountry",
PatientSSN::STRING AS "PatientSSN",
PatientDOB::STRING AS "PatientDOB",
PatientGender::STRING AS "PatientGender",
PatientLanguage::STRING AS "PatientLanguage",
PatientPhone::STRING AS "PatientPhone",
PatientEmailAddress::STRING AS "PatientEmailAddress",
PatientEffectiveFromDate::STRING AS "PatientEffectiveFromDate",
PatientEffectiveToDate::STRING AS "PatientEffectiveToDate",
PatientAccountNumber::STRING AS "PatientAccountNumber",
BillingProviderID::STRING AS "BillingProviderID",
BillingProviderNPI::STRING AS "BillingProviderNPI",
BillingProviderTaxID::STRING AS "BillingProviderTaxID",
BillingProviderName::STRING AS "BillingProviderName",
BillingProviderAddress1::STRING AS "BillingProviderAddress1",
BillingProviderAddress2::STRING AS "BillingProviderAddress2",
BillingProviderCity::STRING AS "BillingProviderCity",
BillingProviderState::STRING AS "BillingProviderState",
BillingProviderZip::STRING AS "BillingProviderZip",
BillingProviderZipPlus::STRING AS "BillingProviderZipPlus",
BillingProviderPhone::STRING AS "BillingProviderPhone",
BillingProviderFax::STRING AS "BillingProviderFax",
BillingProviderEmailAddress::STRING AS "BillingProviderEmailAddress",
BillingProviderContactName::STRING AS "BillingProviderContactName",
BillingProviderContactPhone::STRING AS "BillingProviderContactPhone",
ParticipatingProviderFlag::STRING AS "ParticipatingProviderFlag",
TreatingPhysicianID::STRING AS "TreatingPhysicianID",
TreatingPhysicianNPI::STRING AS "TreatingPhysicianNPI",
TreatingPhysicianTIN::STRING AS "TreatingPhysicianTIN",
TreatingPhysicianName::STRING AS "TreatingPhysicianName",
ClaimSourceSystem::STRING AS "ClaimSourceSystem",
ClaimNumber::STRING AS "ClaimNumber",
billLineNumber::STRING AS "ClaimLineNumber",
AdjustmentIndicator::STRING AS "AdjustmentIndicator",
PreviousClaimNumber::STRING AS "PreviousClaimNumber",
AssignmentIndicator::STRING AS "AssignmentIndicator",
CheckNumber::STRING AS "CheckNumber",
ClaimType::STRING AS "ClaimType",
FormType::STRING AS "FormType",
ServiceType::STRING AS "ServiceType",
TypeOfBill::STRING AS "TypeOfBill",
PlaceOfService::STRING AS "PlaceOfService",
CapitatedClaimFlag::STRING AS "CapitatedClaimFlag",
PaidDate::STRING AS "PaidDate",
DateOfServiceStart::STRING AS "DateOfServiceStart",
DateOfServiceEnd::STRING AS "DateOfServiceEnd",
BilledAmount::STRING AS "BilledAmount",
ReducedAmount::STRING AS "ReducedAmount",
DeductibleAmount::STRING AS "DeductibleAmount",
CoinsuranceAmount::STRING AS "CoinsuranceAmount",
CopayAmount::STRING AS "CopayAmount",
AllowedAmount::STRING AS "AllowedAmount",
PaidAmount::STRING AS "PaidAmount",
ProcedureCodeType::STRING AS "ProcedureCodeType",
ProcedureCode::STRING AS "ProcedureCode",
ProcedureModifierCode1::STRING AS "ProcedureModifierCode1",
ProcedureModifierCode2::STRING AS "ProcedureModifierCode2",
RevenueCode::STRING AS "RevenueCode",
DrugName::STRING AS "DrugName",
DrugType::STRING AS "DrugType",
DrugQuantity::STRING AS "DrugQuantity",
DrugQuantityQualifier::STRING AS "DrugQuantityQualifier",
DrugStrength::STRING AS "DrugStrength",
DrugStrengthUnit::STRING AS "DrugStrengthUnit",
DaysOfSupply::STRING AS "DaysOfSupply",
DAWFlag::STRING AS "DAWFlag",
FormularyStatus::STRING AS "FormularyStatus",
SIG::STRING AS "SIG",
DosageForm::STRING AS "DosageForm",
DrugClassCode::STRING AS "DrugClassCode",
DEAClassCode::STRING AS "DEAClassCode",
PCEmployment::STRING AS "PCEmployment",
PCAutoAccident::STRING AS "PCAutoAccident",
PCAutoAccidentState::STRING AS "PCAutoAccidentState",
PCOtherAccident::STRING AS "PCOtherAccident",
AccidentDate::STRING AS "AccidentDate",
ICDVersion::STRING AS "ICDVersion",
PrimaryDiagCode::STRING AS "PrimaryDiagCode",
AdmittingDiagCode::STRING AS "AdmittingDiagCode",
DiagCode1::STRING AS "DiagCode1",
DiagCode2::STRING AS "DiagCode2",
DiagCode3::STRING AS "DiagCode3",
DiagCode4::STRING AS "DiagCode4",
DiagCode5::STRING AS "DiagCode5",
DiagCode6::STRING AS "DiagCode6",
DiagCode7::STRING AS "DiagCode7",
DiagCode8::STRING AS "DiagCode8",
DiagCode9::STRING AS "DiagCode9",
DiagCode10::STRING AS "DiagCode10",
DiagCode11::STRING AS "DiagCode11",
DiagCode12::STRING AS "DiagCode12",
DiagCode13::STRING AS "DiagCode13",
DiagCode14::STRING AS "DiagCode14",
DiagCode15::STRING AS "DiagCode15",
DiagCode16::STRING AS "DiagCode16",
DiagCode17::STRING AS "DiagCode17",
DiagCode18::STRING AS "DiagCode18",
DiagCode19::STRING AS "DiagCode19",
DiagCode20::STRING AS "DiagCode20",
DiagCode21::STRING AS "DiagCode21",
DiagCode22::STRING AS "DiagCode22",
DiagCode23::STRING AS "DiagCode23",
DiagCode24::STRING AS "DiagCode24",
DiagCode25::STRING AS "DiagCode25",
DiagCode26::STRING AS "DiagCode26",
DiagCode27::STRING AS "DiagCode27",
DiagCode28::STRING AS "DiagCode28",
DiagCode29::STRING AS "DiagCode29",
DiagCode30::STRING AS "DiagCode30",
HealthPlanID::STRING AS "HealthPlanID",
PayerName::STRING AS "PayerName",
PayerState::STRING AS "PayerState",
'''' AS "ClientCustomField1",
'''' AS "ClientCustomField2",
'''' AS "ClientCustomField3",
'''' AS "ClientCustomField4",
'''' AS "ClientCustomField5",
'''' AS "ClientCustomField6",
'''' AS "ClientCustomField7",
'''' AS "ClientCustomField8",
'''' AS "ClientCustomField9",
'''' as "ClientCustomField10"
from IDENTIFIER(:V_final_output_prof_temp);

V_ROWS_LOADED := (select count(1) from IDENTIFIER(:V_final_output_prof)) ;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;


CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, :V_MESSAGE, NULL, NULL); 
                                 
                                 
                                 
V_STEP := ''STEP17'';
   
  
V_STEP_NAME := ''Generate AARP_Inst_Medical_Claims Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      


V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/optum_subrogation/''||''AARP_''||
(SELECT TO_VARCHAR(DATEADD(MONTH, 0, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYYMM''))||''_Inst_Medical_Claims_''||
(SELECT TO_VARCHAR(CURRENT_TIMESTAMP, ''YYYYMMDD_HH24MI''))||''.txt.gz''||'' 

FROM (
             SELECT *
               FROM final_output
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''|''''
			   empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''',''''Null'''',''''null'''',''''00000'''')
               compression = ''''gzip'''' 
              )
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;



execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);    
                                 
                                 
V_STEP := ''STEP18'';
   
  
V_STEP_NAME := ''Generate AARP_Prof_Medical_Claims Report''; 

V_MESSAGE   :=  ''File Generated Successfully'';
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());      



V_STAGE_QUERY := ''COPY INTO ''||:STAGE_NAME||''/pre_clm/outbox/optum_subrogation/''||''AARP_''||
(SELECT TO_VARCHAR(DATEADD(MONTH, 0, DATE_TRUNC(MONTH,:V_CURRENT_DATE)), ''YYYYMM''))||''_Prof_Medical_Claims_''||
(SELECT TO_VARCHAR(CURRENT_TIMESTAMP, ''YYYYMMDD_HH24MI''))||''.txt.gz''||'' 

FROM (
             SELECT *
               FROM final_output_prof
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''|''''
			   empty_field_as_null=false
			   NULL_IF = ('''''''',''''NULL'''', ''''Null'''',''''null'''',''''00000'''')
               compression = ''''gzip'''' 
              )
HEADER = True
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;



execute immediate ''USE SCHEMA ''||:TGT_SC;                                  
execute immediate  :V_STAGE_QUERY;  


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, NULL, NULL, :V_MESSAGE, NULL, NULL);                                       
                                 
                                 


EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';