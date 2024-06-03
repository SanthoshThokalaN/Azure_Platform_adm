USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_CHP_TESTBED_MAIN("CLH_TRK_ID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
p2 VARCHAR; 
V_STAGE_QUERY VARCHAR;
  

BEGIN

p2 := :CLH_TRK_ID;

                       

INSERT INTO SRC_EDI_837.CH_P_FINAL
SELECT DISTINCT concat (''~ST*837'', clmdata) as clmdata, clm_date FROM SRC_EDI_837.CHP_CLAIM
WHERE contains(clmdata, concat(''REF*D9*'', :p2));


INSERT INTO SRC_EDI_837.CH_P_MERGE_FINAL
SELECT DISTINCT concat (clm_data, ''~'') as clmdata, clm_date, (SELECT SUBSTR(CLM_NUM, 6, 3) FROM SRC_EDI_837.CHP_TESTBED_DATA WHERE CLH_TRK_ID = :p2) as loc FROM SRC_EDI_837.CH_P_FINAL
WHERE contains(clmdata, concat(''REF*D9*'', :p2));


CREATE OR REPLACE TRANSIENT TABLE SRC_EDI_837.CHP_ITERATIVE AS
SELECT DISTINCT concat (''~ST*837'', clmdata, ''~'') as clmdata, clm_date FROM SRC_EDI_837.CHP_CLAIM
WHERE contains(clmdata, concat(''REF*D9*'', :p2));


V_STAGE_QUERY := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chp/'' || ''CH_837P_'' || (SELECT DISTINCT MEMBER_ID from SRC_EDI_837.CHP_TESTBED_DATA WHERE CLH_TRK_ID = :p2) || ''_'' ||(SELECT CLM_NUM FROM SRC_EDI_837.CHP_TESTBED_DATA WHERE CLH_TRK_ID = :p2)||''_''||(SELECT clm_date from SRC_EDI_837.CHP_ITERATIVE)||''_''||(SELECT DISTINCT FILENAME from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP) ||'' FROM (
           select clmdata from SRC_EDI_837.CHP_ITERATIVE
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = none
               record_delimiter = none
               compression = None 
              )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;
 

IF ((SELECT COUNT(clmdata) FROM SRC_EDI_837.CHP_ITERATIVE) > 0) THEN
execute immediate  :V_STAGE_QUERY; 
ELSE
RETURN ''SUCCESS'';
END IF;


END;

';