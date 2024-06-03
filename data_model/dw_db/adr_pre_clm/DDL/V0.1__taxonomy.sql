USE SCHEMA ADR_PRE_CLM;

create or replace view V_TAXONOMY_MISMATCH_RAW(
	UCPS_CLM_NUM,
	BILL_LINE,
	CPT_CODES,
	MED_APPROVED_AMOUNT,
	TAXONOMY_CODES,
	PROVIDER_TAX_ID,
	PROVIDER_NPI,
	ADJ_BEN_AMT,
	CLAIM_PD_DATE
) as 

(select
UCPS_CLM_NUM,
BILL_LINE,
CPT_CODES,
MED_APPROVED_AMOUNT,
TAXONOMY_CODES,
PROVIDER_TAX_ID,
PROVIDER_NPI,
ADJ_BEN_AMT,
CLAIM_PD_DATE
      from SRC_EDI_837.TAXONOMY_MISMATCH_RAW );

create or replace view V_D_CPT_LOOK(
	D_CPT_LOOK_SK,
	CPT_CD,
	ETL_LST_BTCH_ID,
	CPT_TYP_CD,
	AHRQ_PROC_DTL_CATGY_CD,
	AHRQ_PROC_DTL_CATGY_DESC,
	AHRQ_PROC_GENL_CATGY_CD,
	AHRQ_PROC_GENL_CATGY_DESC,
	ASG_GRP_CD,
	PROC_DECM_CD,
	PROC_DESC,
	PROC_FULL_DESC,
	PROC_LNG_DESC,
	SRVC_CATGY_CD,
	SRVC_CATGY_DESC,
	GDR_SPEC_CD,
	VST_CD,
	SRC_LOAD_DT,
	SRC_UPDT_DT,
	PROC_END_DT,
	ROW_INSRT_DT,
	ROW_UPDT_DT,
	CPT_CATGY_CD
) as 
select 
D_CPT_LOOK_SK,
CPT_CD        ,            
ETL_LST_BTCH_ID,           
CPT_TYP_CD      ,          
AHRQ_PROC_DTL_CATGY_CD    ,
AHRQ_PROC_DTL_CATGY_DESC  ,
AHRQ_PROC_GENL_CATGY_CD   ,
AHRQ_PROC_GENL_CATGY_DESC ,
ASG_GRP_CD                ,
PROC_DECM_CD              ,
PROC_DESC                 ,
PROC_FULL_DESC            ,
PROC_LNG_DESC             ,
SRVC_CATGY_CD             ,
SRVC_CATGY_DESC           ,
GDR_SPEC_CD               ,
VST_CD                    ,
SRC_LOAD_DT               ,
SRC_UPDT_DT               ,
PROC_END_DT               ,
ROW_INSRT_DT              ,
ROW_UPDT_DT               ,
CPT_CATGY_CD              
from
"SRC_ISDW"."D_CPT_LOOK";


create or replace view V_TAXONOMY_DESCRIPTION(
	TAXONOMY_CODE,
	CODE_GROUPING,
	CLASSIFICATION,
	SPECIALIZATION,
	DEFINITION,
	NOTES
) as 
select taxonomy_code,code_grouping,classification,specialization,definition,notes
from "SRC_ISDW"."TAXONOMY_DESCRIPTION";
