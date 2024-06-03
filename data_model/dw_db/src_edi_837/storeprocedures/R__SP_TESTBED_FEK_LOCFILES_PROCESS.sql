USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_TESTBED_FEK_LOCFILES_PROCESS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  meta_cursor CURSOR FOR
    SELECT DISTINCT LOC AS LOC  FROM SRC_EDI_837.FEK_P_MERGE_FINAL ;
  meta_cursor2 CURSOR FOR
    SELECT DISTINCT LOC AS LOC  FROM SRC_EDI_837.FEK_I_MERGE_FINAL ;   
  LOC INT;
  my_sql VARCHAR;
  V_STAGE_QUERY1 VARCHAR;
BEGIN
  OPEN meta_cursor;
  FOR myrow IN meta_cursor DO
    LOC := myrow.LOC;
    my_sql := ''create or replace table SRC_EDI_837.FEK_CLAIM_INDIVIDUAL_FILES AS select distinct(CLMdata) from SRC_EDI_837.FEK_P_MERGE_FINAL where   LOC= '' || :LOC;
    V_STAGE_QUERY1 := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/fek/fekp/FEK_837P_Merged_''||:LOC||''_''||(SELECT to_varchar(CURRENT_DATE,''YYYYMMDD''))||''_''||(SELECT DISTINCT file from SRC_EDI_837.TESTBED_FEK_INPUT_CLAIMS_TMP)||'' FROM SRC_EDI_837.FEK_CLAIM_INDIVIDUAL_FILES 
                       file_format = (type = ''''CSV'''' 
                       field_delimiter = ''''@''''
                       record_delimiter = None
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
  
 OPEN meta_cursor2;
  FOR myrow IN meta_cursor2 DO
    LOC := myrow.LOC;
    my_sql := ''create or replace table SRC_EDI_837.FEK_CLAIM_INDIVIDUAL_FILES AS select distinct(CLMdata) from SRC_EDI_837.FEK_I_MERGE_FINAL where   LOC= '' || :LOC;
    V_STAGE_QUERY1 := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/fek/feki/FEK_837I_Merged_''||:LOC||''_''||(SELECT to_varchar(CURRENT_DATE,''YYYYMMDD''))||''_''||(SELECT DISTINCT file from SRC_EDI_837.TESTBED_FEK_INPUT_CLAIMS_TMP)||'' FROM SRC_EDI_837.FEK_CLAIM_INDIVIDUAL_FILES 
                       file_format = (type = ''''CSV'''' 
                       field_delimiter = ''''@''''
                       record_delimiter = None                       
               compression = None 
              )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True'';
 
 
EXECUTE IMMEDIATE my_sql;
execute immediate  :V_STAGE_QUERY1; 

  
  END FOR;
  CLOSE meta_cursor2; 
  


  RETURN ''Update successful'';
END;
';