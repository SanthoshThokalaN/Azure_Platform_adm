
USE SCHEMA LZ_COMPAS;

CREATE TRANSIENT TABLE IF NOT EXISTS CLG_MSG (
	MSG_DESC VARCHAR(16777216),
	INBOUND VARCHAR(16777216),
	MSG_CATEGORY_ID VARCHAR(16777216),
	MSG_TYPE_CREATED_BY VARCHAR(16777216),
	MSG_TYPE_CREATION_DATE VARCHAR(16777216),
	MSG_TYPE_LAST_MODIFIED_BY VARCHAR(16777216),
	MSG_TYPE_LAST_MODIFIED_DATE VARCHAR(16777216),
	MSG_LOG_ID VARCHAR(16777216),
	BODID VARCHAR(16777216),
	MSG_STATUS_ID VARCHAR(16777216),
	REFERENCE_ID VARCHAR(16777216),
	MSG_LOG_CREATE_DATE VARCHAR(16777216),
	MSG_LOG_CREATED_BY VARCHAR(16777216),
	MSG_LOG_CREATION_DATE VARCHAR(16777216),
	MSG_TEXT_ID VARCHAR(16777216),
	MSG_CLOB VARCHAR(16777216),
	MSG_LOG_CREATION_DATE_DL VARCHAR(16777216),
	MSG_TYPE_ID VARCHAR(16777216)
);