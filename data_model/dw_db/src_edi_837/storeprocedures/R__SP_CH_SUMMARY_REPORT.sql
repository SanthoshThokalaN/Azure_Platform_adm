USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_CH_SUMMARY_REPORT()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_STAGE_QUERY              VARCHAR; 

  

BEGIN
                               

V_STAGE_QUERY := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/''||''summary_'' ||(SELECT DISTINCT filename from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP)||'' FROM (
          select ''''<response>''''
          UNION 
          select  concat(''''<receivedClaimCount>'''',(Select count(*) from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP),''''</receivedClaimCount>'''')
          UNION
          select  concat(''''<existClaimCount>'''',(SELECT COUNT (DISTINCT CLM_DATA ) FROM (
Select * from SRC_EDI_837.CH_P_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_I_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_VP_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_VI_FINAL
)),''''</existClaimCount>'''')
          
          UNION
          
          
          select  concat(''''<missedClaimCount>'''',(select ((Select count(*) from SRC_EDI_837.TESTBED_INPUT_CLAIMS_TMP) -(SELECT COUNT (DISTINCT CLM_DATA ) FROM (
Select * from SRC_EDI_837.CH_P_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_I_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_VP_FINAL
UNION ALL
Select * from SRC_EDI_837.CH_VI_FINAL
) ))),''''<missedClaimCount>'''')
                  
 
     
          
          
          UNION
          select  concat(''''<mergedFileCounts>'''',(SELECT 
          (
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chi/ WHERE metadata$filename LIKE ''''%CH_837I_Merged%'''') + 
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chp/ WHERE metadata$filename LIKE ''''%CH_837P_Merged%'''') + 
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chvp/ WHERE metadata$filename LIKE ''''%CH_V837P_Merged%'''') + 
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/ch/chvi/ WHERE metadata$filename LIKE ''''%CH_V837I_Merged%'''')
          
          )),''''</mergedFileCounts>'''')
          union
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
