USE SCHEMA SRC_EDI_837;


CREATE OR REPLACE PROCEDURE SP_FEK_SUMMARY_REPORT()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_STAGE_QUERY              VARCHAR; 

  

BEGIN
                               

V_STAGE_QUERY := ''COPY INTO ''||''@UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/fek/''||''summary_'' ||(SELECT DISTINCT rtrim(file, ''.txt'') from SRC_EDI_837.TESTBED_FEK_INPUT_CLAIMS_TMP)||''.xml''||'' FROM (
          select ''''<response>''''
          UNION ALL 
          select  concat(''''<receivedClaimCount>'''',(Select count(*) from SRC_EDI_837.TESTBED_FEK_INPUT_CLAIMS_TMP),''''</receivedClaimCount>'''')
          UNION ALL
          select  concat(''''<existClaimCount>'''',(SELECT COUNT (DISTINCT CLM_DATA ) FROM (
Select * from SRC_EDI_837.FEK_P_FINAL
UNION ALL
Select * from SRC_EDI_837.FEK_I_FINAL
)),''''</existClaimCount>'''')

          UNION ALL
          
          
          select  concat(''''<missedClaimCount>'''',(select ((Select count(*) from SRC_EDI_837.TESTBED_FEK_INPUT_CLAIMS_TMP) -(SELECT COUNT (DISTINCT CLM_DATA ) FROM (
Select * from SRC_EDI_837.FEK_P_FINAL
UNION ALL
Select * from SRC_EDI_837.FEK_I_FINAL
) ))),''''</missedClaimCount>'''')
          
 
     
          
          
          UNION ALL
          select  concat(''''<mergedFileCounts>'''',(SELECT 
          (
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/fek/feki/ WHERE metadata$filename LIKE ''''%FEK_837I_Merged%'''') + 
          (select COUNT(*) FROM @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/final_output/fek/fekp/ WHERE metadata$filename LIKE ''''%FEK_837P_Merged%'''')
          
          )),''''</mergedFileCounts>'''')
          UNION ALL
          select ''''</response>''''
)
file_format = (type = ''''CSV''''
               compression = None
               )
HEADER = FALSE
OVERWRITE = True
MAX_FILE_SIZE = 4900000000
SINGLE = True;''
;
                                 
IF ( (SELECT COUNT(file) from SRC_EDI_837.TESTBED_FEK_INPUT_CLAIMS_TMP) > 0 ) THEN
execute immediate  :V_STAGE_QUERY; 
ELSE
RETURN ''SUCCESS'';
END IF;  

END;

';