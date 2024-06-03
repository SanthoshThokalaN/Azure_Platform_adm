USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_TESTBED_PYSPARK2("REF_CLAIM_NUMBER" VARCHAR(16777216), "MEMBER_ID" VARCHAR(16777216), "CLM_RECEPT_DT" VARCHAR(16777216), "CLM_NUM" VARCHAR(16777216), "MEDICARE_CLM_CNTRL_NUM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
p1 VARCHAR;
p2 VARCHAR;
p3 varchar;
p4 varchar;
p5 varchar;
p6 varchar;
p7 varchar;
V_STAGE_QUERY1 VARCHAR;
V_LAST_QUERY_ID VARCHAR;
begin
p1 := :REF_CLAIM_NUMBER;
p2 := :MEMBER_ID;
p3 := substr(:p1,9,length(:p1));
p4 := :CLM_RECEPT_DT;
p5 := :CLM_NUM;
p6 := :MEDICARE_CLM_CNTRL_NUM;
p7 := to_varchar(p4::date,''YYYYMMDD'');

INSERT INTO SRC_EDI_837.TESTBED_FILTER_PYSPARK2 select Replace(Replace(regexp_replace(concat(col0,col1,col2,col3,''~'',PROF_COL0), ''[\\"]'', ''''), char(10), ''''), char(13), '''') as final_data, :p5 as CLM_NUM from SRC_EDI_837.PROF_STRSPLIT_SE where CONTAINS(COL3,:p1) and CONTAINS(COL2,:p2) OR CONTAINS(COL3,:p2);
 
TRUNCATE TABLE SRC_EDI_837.TESTBED_FILTER_ITERATIONS;
 
INSERT INTO SRC_EDI_837.TESTBED_FILTER_ITERATIONS select Replace(Replace(regexp_replace(concat(col0,col1,col2,col3,''~'',PROF_COL0), ''[\\"]'', ''''), char(10), ''''), char(13), '''') as final_data from SRC_EDI_837.PROF_STRSPLIT_SE where CONTAINS(COL3,:p1) and CONTAINS(COL2,:p2) OR CONTAINS(COL3,:p2);
 
 
V_STAGE_QUERY1 := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/cms/CMS_837P_''||:p3||''_''||:p5||''_''||:p7||''_''||(SELECT DISTINCT filename from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)||'' FROM (
           select distinct *   from SRC_EDI_837.TESTBED_FILTER_ITERATIONS where final_data like ''''%REF*F8%''''
               )
file_format = (type = ''''CSV'''' 
               field_delimiter = ''''
''''
               compression = None 
              )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True''
;
 
IF ((SELECT COUNT(*) FROM SRC_EDI_837.TESTBED_FILTER_ITERATIONS where final_data like ''%REF*F8%'') > 0) THEN
execute immediate  :V_STAGE_QUERY1; 
ELSE
RETURN ''SUCCESS'';
END IF;
V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
 
 
END;
';
