USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_FEKI_TESTBED_MAIN("DOC_CTL_NBR" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
p2 VARCHAR; 
V_STAGE_QUERY VARCHAR;

  

BEGIN

p2 := :DOC_CTL_NBR;

INSERT INTO SRC_EDI_837.FEK_I_FINAL
SELECT DISTINCT concat (''~ST*837'', clmdata) as clmdata, clm_date FROM SRC_EDI_837.FEKI_CLAIM
WHERE contains(clmdata, concat(''f_'', :p2));                      

INSERT INTO SRC_EDI_837.FEK_I_MERGE_FINAL
SELECT DISTINCT concat (clm_data, ''~'') as clmdata, clm_date, (SELECT SUBSTR(UCPS_CLM_NUM, 6, 3) FROM SRC_EDI_837.FEKI_TESTBED_DATA WHERE DOC_CTL_NBR = :p2) as loc FROM SRC_EDI_837.FEK_I_FINAL
WHERE contains(clmdata, concat(''f_'', :p2));


CREATE OR REPLACE TRANSIENT TABLE SRC_EDI_837.FEKI_ITERATIVE AS
SELECT DISTINCT concat (''~ST*837'', clmdata, ''~'') as clmdata, clm_date FROM SRC_EDI_837.FEKI_CLAIM
WHERE contains(clmdata, concat(''f_'', :p2));


V_STAGE_QUERY := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/fek/feki/'' || ''FEK_837I_'' || (SELECT DISTINCT MEMBER_ID from SRC_EDI_837.FEKI_TESTBED_DATA WHERE DOC_CTL_NBR = :p2) || ''_'' ||(SELECT UCPS_CLM_NUM FROM SRC_EDI_837.FEKI_TESTBED_DATA WHERE DOC_CTL_NBR = :p2)||''_''||(SELECT clm_date from SRC_EDI_837.FEKI_ITERATIVE)||''_''||(SELECT DISTINCT file from SRC_EDI_837.TESTBED_FEK_INPUT_CLAIMS_TMP) ||'' FROM (
           select DISTINCT clmdata from SRC_EDI_837.FEKI_ITERATIVE
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
 
                                 
IF ((SELECT COUNT(clmdata) FROM SRC_EDI_837.FEKI_ITERATIVE) > 0) THEN
execute immediate  :V_STAGE_QUERY; 
ELSE
RETURN ''SUCCESS'';
END IF;


END;

';