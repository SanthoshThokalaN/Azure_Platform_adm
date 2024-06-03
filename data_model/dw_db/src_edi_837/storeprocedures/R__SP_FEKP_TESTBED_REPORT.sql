USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_FEKP_TESTBED_REPORT()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_STAGE_QUERY              VARCHAR; 
V_REPORT_TIME               VARCHAR;

  

BEGIN

V_REPORT_TIME := TO_VARCHAR(CURRENT_TIMESTAMP(), ''yyyymmdd'');
                               

V_STAGE_QUERY := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/fek/fekp/'' || ''FEK_837P_Merged_'' || :V_REPORT_TIME || ''_'' || (SELECT DISTINCT file from SRC_EDI_837.TESTBED_FEK_INPUT_CLAIMS_TMP)||'' FROM (
           select CLM_DATA from SRC_EDI_837.FEK_P_FINAL
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
 
                                 
IF ((SELECT COUNT(clm_data) FROM SRC_EDI_837.FEK_P_FINAL) > 0) THEN
execute immediate  :V_STAGE_QUERY;  
ELSE
RETURN ''SUCCESS'';
END IF; 

END;

';