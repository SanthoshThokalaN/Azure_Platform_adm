
USE SCHEMA SRC_COMPAS;
create  TABLE IF NOT EXISTS GET_CUSTOMER_CALLS (
	INDIVIDUAL_ID VARCHAR(16777216),
	NUMBER_OF_CALLS VARCHAR(16777216),
	CALL_DATE VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9)
);


create  TABLE IF NOT EXISTS CLG_MSG_XML (
	MSG_DESC VARCHAR(16777216),
	INBOUND BOOLEAN,
	MSG_CATEGORY_ID NUMBER(38,0),
	MSG_TYPE_CREATED_BY VARCHAR(16777216),
	MSG_TYPE_CREATION_DATE TIMESTAMP_NTZ(9),
	MSG_TYPE_LAST_MODIFIED_BY VARCHAR(16777216),
	MSG_TYPE_LAST_MODIFIED_DATE TIMESTAMP_NTZ(9),
	MSG_LOG_ID NUMBER(38,0),
	BODID VARCHAR(16777216),
	MSG_STATUS_ID NUMBER(38,0),
	REFERENCE_ID VARCHAR(16777216),
	MSG_LOG_CREATE_DATE TIMESTAMP_NTZ(9),
	MSG_LOG_CREATED_BY VARCHAR(16777216),
	MSG_LOG_CREATION_DATE TIMESTAMP_NTZ(9),
	MSG_TEXT_ID NUMBER(38,0),
	MSG_CLOB OBJECT,
	MSG_LOG_CREATION_DATE_DL DATE,
	MSG_TYPE_ID NUMBER(38,0),
  	ISDC_CREATED_DT TIMESTAMP_NTZ(9),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9)
);
