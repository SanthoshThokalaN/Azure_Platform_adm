USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_TESTBED()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var row_count_query=snowflake.createStatement({sqlText: "select count(distinct metadata$filename) as Count from @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/split/"});
var result_set=row_count_query.execute(); result_set.next();
var Last_Value=result_set.getColumnValue(1);
var i=0;
 for (i=0; i < Last_Value; i++) 
 {
 var query=`select concat(''''''''||''select $1  as value from @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/split/''||substr(metadata$filename,44,20)||'' (file_format => ''''''''SRC_EDI_837.ff_testbed837'''''''')''||'''''''') as filename from @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/split/ order by filename limit 1 offset ${i}`;
var statement = snowflake.createStatement({sqlText: query});
var result= statement.execute(); result.next();
var param =result.getColumnValue(1);
var proc= `call SRC_EDI_837.SP_CMS_TESTBED_PYSPARK1_MAIN(${param})`;
var proc_statement = snowflake.createStatement({sqlText: proc});
var proc_result= proc_statement.execute(); proc_result.next();
    }
return proc_result;
';