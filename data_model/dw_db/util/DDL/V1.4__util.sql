
USE SCHEMA UTIL;
CREATE OR REPLACE FILE FORMAT FF_TILDA_CSV
	FIELD_DELIMITER = '~'
	SKIP_HEADER = 0
	ESCAPE_UNENCLOSED_FIELD = 'NONE'
	TRIM_SPACE = TRUE
	NULL_IF = ('null', 'NULL', '\N', '\\N')
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	ENCODING = 'WINDOWS1252'

;
