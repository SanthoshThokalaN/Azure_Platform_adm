USE SCHEMA UTIL;
CREATE OR REPLACE FILE FORMAT FF_DATA_EXTRACT_PIPE_CSV
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	ESCAPE_UNENCLOSED_FIELD = 'NONE'
	TRIM_SPACE = TRUE
	NULL_IF = ('null', 'NULL', 'N', '\\N')
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	ENCODING = 'WINDOWS1252'
    COMMENT = 'Usage: Load on prem data to snowflake'
;


CREATE OR REPLACE FILE FORMAT FF_DATA_EXTRACT_PIPE_CSV_COMPAS
	FIELD_DELIMITER = '|'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
;

CREATE OR REPLACE FILE FORMAT FIX_WIDTH_BILLING
	FIELD_DELIMITER = 'NONE'
	SKIP_HEADER = 1
;