USE SCHEMA UTIL;

DROP FILE FORMAT FIX_WIDTH_BILLING;

CREATE OR REPLACE FILE FORMAT FF_FIX_WIDTH_BILLING
	FIELD_DELIMITER = 'NONE'
	SKIP_HEADER = 1
;

