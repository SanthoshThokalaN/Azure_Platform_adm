use schema SRC_ISDW;

create TABLE IF NOT EXISTS MED_APPROVED_AMT (
	CLAIM_NUMBER VARCHAR(16777216),
	BILL_LINE_NUMBER VARCHAR(16777216),
	MEDICARE_APPROVED_AMOUNT FLOAT,
	ETL_LST_BTCH_ID VARCHAR(16777216),
	ADJ_BEN_AMT FLOAT,
	CLAIM_PD_DATE VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9)
);

create TABLE IF NOT EXISTS D_CPT_LOOK (
	D_CPT_LOOK_SK NUMBER(38,0),
	CPT_CD VARCHAR(5),
	ETL_LST_BTCH_ID NUMBER(38,0),
	CPT_TYP_CD VARCHAR(16777216),
	AHRQ_PROC_DTL_CATGY_CD VARCHAR(16777216),
	AHRQ_PROC_DTL_CATGY_DESC VARCHAR(16777216),
	AHRQ_PROC_GENL_CATGY_CD VARCHAR(16777216),
	AHRQ_PROC_GENL_CATGY_DESC VARCHAR(16777216),
	ASG_GRP_CD VARCHAR(16777216),
	PROC_DECM_CD VARCHAR(16777216),
	PROC_DESC VARCHAR(16777216),
	PROC_FULL_DESC VARCHAR(16777216),
	PROC_LNG_DESC VARCHAR(16777216),
	SRVC_CATGY_CD VARCHAR(16777216),
	SRVC_CATGY_DESC VARCHAR(16777216),
	GDR_SPEC_CD VARCHAR(16777216),
	VST_CD VARCHAR(16777216),
	SRC_LOAD_DT VARCHAR(16777216),
	SRC_UPDT_DT VARCHAR(16777216),
	PROC_END_DT VARCHAR(16777216),
	ROW_INSRT_DT VARCHAR(16777216),
	ROW_UPDT_DT VARCHAR(16777216),
	CPT_CATGY_CD VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9)
);