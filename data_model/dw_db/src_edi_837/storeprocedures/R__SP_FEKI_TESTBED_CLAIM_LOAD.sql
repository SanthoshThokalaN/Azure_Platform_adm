USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_FEKI_TESTBED_CLAIM_LOAD("FILE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

p1 VARCHAR;

V_STAGE_QUERY              VARCHAR; 

  

BEGIN

p1 := :FILE_NAME;

TRUNCATE TABLE SRC_EDI_837.FEKI_CLAIM_TEMP;                               
                               

V_STAGE_QUERY := ''COPY INTO SRC_EDI_837.FEKI_CLAIM_TEMP FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/initial/fek/FEK_837IS_O.v0/''|| :p1 ||'' file_format = (type = ''''CSV'''' 
record_delimiter = ''''~ST*837''''
field_delimiter = None
escape = ''''
'''')'';
                          
execute immediate :V_STAGE_QUERY;


INSERT INTO SRC_EDI_837.FEKI_CLAIM
SELECT *, substr(:p1,13,8) FROM SRC_EDI_837.FEKI_CLAIM_TEMP;

END;

';