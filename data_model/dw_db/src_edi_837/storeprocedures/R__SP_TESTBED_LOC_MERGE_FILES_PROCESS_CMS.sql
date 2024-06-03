USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_TESTBED_LOC_MERGE_FILES_PROCESS_CMS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  meta_cursor CURSOR FOR
    SELECT DISTINCT SUBSTR(CLM_NUM,6,3) AS CLM_NUM  FROM SRC_EDI_837.TESTBED_FILTER_PYSPARK2 GROUP BY SUBSTR(CLM_NUM,6,3);
  CLM_NUM INT;
  my_sql VARCHAR;
  V_STAGE_QUERY1 VARCHAR;
  V_STAGE_QUERY_SUMMARY VARCHAR;
  V_STAGE_QUERY_MERGEFILE VARCHAR;
BEGIN
  OPEN meta_cursor;
  FOR myrow IN meta_cursor DO
    CLM_NUM := myrow.CLM_NUM;
    my_sql := ''create or replace table SRC_EDI_837.CLAIM_INDIVIDUAL_FILES AS select distinct(final_data) from SRC_EDI_837.TESTBED_FILTER_PYSPARK2 where final_data like ''''%REF*F8*%'''' and  SUBSTR(CLM_NUM,6,3)= '' || :CLM_NUM;
   
   
    V_STAGE_QUERY1 := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/cms/CMS_837P_Merged_''||:CLM_NUM||''_''||(SELECT to_varchar(CURRENT_DATE,''YYYYMMDD''))||''_''||(SELECT DISTINCT filename from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)||'' FROM SRC_EDI_837.CLAIM_INDIVIDUAL_FILES
                       file_format = (type = ''''CSV''''
                       field_delimiter = None
                       record_delimiter= None
               compression = None
              )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True'';
 
EXECUTE IMMEDIATE my_sql;
execute immediate  :V_STAGE_QUERY1;
 
 
  END FOR;
  CLOSE meta_cursor;
 
 
   V_STAGE_QUERY_MERGEFILE :=  ''copy into ''|| ''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/cms/CMS_837P_Merged_''||(SELECT to_char(current_timestamp,''YYYYMMDD''))||(SELECT DISTINCT filename from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)|| '' from (
   select distinct *   from SRC_EDI_837.TESTBED_FILTER_PYSPARK2 where final_data like ''''%REF*F8*%''''
)
file_format = (type = ''''CSV''''
               field_delimiter=None
               record_delimiter = None
               compression = None
              )
HEADER = FALSE
OVERWRITE = TRUE
MAX_FILE_SIZE = 49000000
SINGLE = TRUE'';
 
 
execute immediate  :V_STAGE_QUERY_MERGEFILE;
 
 
  RETURN ''Update successful'';
END;
';