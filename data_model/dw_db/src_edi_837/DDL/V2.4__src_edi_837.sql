USE SCHEMA SRC_EDI_837;

create or replace TRANSIENT TABLE EXELA_AUD_DATA (
	USER_NM VARCHAR(16777216),
	ACT_DT VARCHAR(16777216),
	AUD_TRL_DTL VARCHAR(16777216),
	AUD_TRL_DESC VARCHAR(16777216),
	OBJECT_ID INT,
	USER_ID VARCHAR(16777216)
);

