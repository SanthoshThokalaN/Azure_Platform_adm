USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_CH_CMS_SUMMARY_REPORT()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_STAGE_QUERY              VARCHAR; 

  

BEGIN
                               

V_STAGE_QUERY := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/''||''summary_'' ||(SELECT DISTINCT rtrim(filename, ''.txt'') from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)||''.xml''||'' FROM (
          select ''''<response>''''
          UNION 
          select  concat(''''<receivedClaimCount>'''',(Select count(*) from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP),''''</receivedClaimCount>'''')
          UNION
          select  concat(''''<existClaimCount>'''',(select(SELECT COUNT (DISTINCT CLM_DATA ) FROM (
Select * from SRC_EDI_837.CH_P_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_I_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_VP_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_VI_FINAL
)) + (select count(distinct final_data )   from SRC_EDI_837.TESTBED_FILTER_PYSPARK2 where final_data like ''''%REF*F8%'''')),''''</existClaimCount>'''')
          
          UNION
          
          
          select  concat(''''<missedClaimCount>'''',(select ((Select count(*) from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP) - (select(SELECT COUNT (DISTINCT CLM_DATA ) FROM (
Select * from SRC_EDI_837.CH_P_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_I_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_VP_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_VI_FINAL
)) + (select count(distinct final_data )   from SRC_EDI_837.TESTBED_FILTER_PYSPARK2 where final_data like ''''%REF*F8%'''')))),''''</missedClaimCount>'''')
                  
 
     
          
          
          UNION
          select  concat(''''<mergedFileCounts>'''',(SELECT 
          (
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chi/ WHERE metadata$filename LIKE ''''%CH_837I_Merged%'''') + 
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chp/ WHERE metadata$filename LIKE ''''%CH_837P_Merged%'''') + 
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chvp/ WHERE metadata$filename LIKE ''''%CH_V837P_Merged%'''') + 
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chvi/ WHERE metadata$filename LIKE ''''%CH_V837I_Merged%'''') + 
		  (select count(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/cms/ WHERE metadata$filename LIKE ''''%CMS_837P_Merged%'''')
           )),''''</mergedFileCounts>'''')
          union all
          select ''''</response>''''
)
file_format = (type = ''''CSV''''
               field_delimiter = None
               compression = None
               )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True;''
;
                                 
IF ( (SELECT COUNT(filename) from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP) > 0 ) THEN
execute immediate  :V_STAGE_QUERY; 
ELSE
RETURN ''SUCCESS'';
END IF;   

END;

';
