USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_TESTBED_CH_LOCFILES_PROCESS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  meta_cursor CURSOR FOR
    SELECT DISTINCT LOC AS LOC  FROM SRC_EDI_837.CH_P_MERGE_FINAL ;
  meta_cursor2 CURSOR FOR
    SELECT DISTINCT LOC AS LOC  FROM SRC_EDI_837.CH_I_MERGE_FINAL ;   
  meta_cursor3 CURSOR FOR
    SELECT DISTINCT LOC AS LOC  FROM SRC_EDI_837.CH_VP_MERGE_FINAL ;  
  meta_cursor4 CURSOR FOR
    SELECT DISTINCT LOC AS LOC  FROM SRC_EDI_837.CH_VI_MERGE_FINAL ;
  LOC INT;
  my_sql VARCHAR;
  V_STAGE_QUERY1 VARCHAR;
BEGIN
  OPEN meta_cursor;
  FOR myrow IN meta_cursor DO
    LOC := myrow.LOC;
    my_sql := ''create or replace table SRC_EDI_837.CH_P_CLAIM_INDIVIDUAL_FILES AS select distinct(CLMDATA) from SRC_EDI_837.CH_P_MERGE_FINAL where   LOC= '' || :LOC;
    V_STAGE_QUERY1 := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chp/CH_837P_Merged_''||:LOC||''_''||(SELECT to_varchar(CURRENT_DATE,''YYYYMMDD''))||''_''||(SELECT DISTINCT filename from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)||'' FROM SRC_EDI_837.CH_P_CLAIM_INDIVIDUAL_FILES 
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
    my_sql := ''create or replace table SRC_EDI_837.CH_I_CLAIM_INDIVIDUAL_FILES AS select distinct(CLMdata) from SRC_EDI_837.CH_I_MERGE_FINAL where   LOC= '' || :LOC;
    V_STAGE_QUERY1 := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chi/CH_837I_Merged_''||:LOC||''_''||(SELECT to_varchar(CURRENT_DATE,''YYYYMMDD''))||''_''||(SELECT DISTINCT filename from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)||'' FROM SRC_EDI_837.CH_I_CLAIM_INDIVIDUAL_FILES 
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
  
  OPEN meta_cursor3;
  FOR myrow IN meta_cursor3 DO
    LOC := myrow.LOC;
    my_sql := ''create or replace table SRC_EDI_837.CH_VP_CLAIM_INDIVIDUAL_FILES AS select distinct(CLMdata) from SRC_EDI_837.CH_VP_MERGE_FINAL where   LOC= '' || :LOC;
    V_STAGE_QUERY1 := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chvp/CH_837VP_Merged_''||:LOC||''_''||(SELECT to_varchar(CURRENT_DATE,''YYYYMMDD''))||''_''||(SELECT DISTINCT filename from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)||'' FROM SRC_EDI_837.CH_VP_CLAIM_INDIVIDUAL_FILES 
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
  CLOSE meta_cursor3; 
  
  OPEN meta_cursor4;
  FOR myrow IN meta_cursor4 DO
    LOC := myrow.LOC;
    my_sql := ''create or replace table SRC_EDI_837.CH_VI_CLAIM_INDIVIDUAL_FILES AS select distinct(CLMdata) from SRC_EDI_837.CH_VI_MERGE_FINAL where   LOC= '' || :LOC;
    V_STAGE_QUERY1 := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chvi/CH_837VI_Merged_''||:LOC||''_''||(SELECT to_varchar(CURRENT_DATE,''YYYYMMDD''))||''_''||(SELECT DISTINCT filename from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)||'' FROM SRC_EDI_837.CH_VI_CLAIM_INDIVIDUAL_FILES 
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
  CLOSE meta_cursor4; 
  


  RETURN ''Update successful'';
END;
';