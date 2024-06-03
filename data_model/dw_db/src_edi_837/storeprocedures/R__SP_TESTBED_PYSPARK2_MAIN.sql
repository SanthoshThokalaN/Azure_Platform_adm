USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_TESTBED_PYSPARK2_MAIN()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var row_count_query=snowflake.createStatement({sqlText: "select count(*) from src_edi_837.testbedinput"});
var result_set=row_count_query.execute(); result_set.next();
var Last_Value=result_set.getColumnValue(1);
var i=0;
for (i=0; i < Last_Value; i++) 
{
var proc_1 = `select  concat(''~REF\\*F8\\*'',medicare_clm_cntrl_num),member_id,clm_recept_dt,clm_num,medicare_clm_cntrl_num from src_edi_837.testbedinput ORDER BY medicare_clm_cntrl_num limit 1 offset ${i}`;
var statement=snowflake.createStatement({sqlText: proc_1});
var result= statement.execute(); result.next();
var param1 =result.getColumnValue(1);
var param2 =result.getColumnValue(2);
var param3 =result.getColumnValue(3);
var param4 =result.getColumnValue(4);
var param5 =result.getColumnValue(5);
var proc= `call SRC_EDI_837.SP_TESTBED_PYSPARK2(''${param1}'',''${param2}'',''${param3}'',''${param4}'',''${param5}'')`;
var proc_statement = snowflake.createStatement({sqlText: proc});
var proc_result= proc_statement.execute(); 
proc_result.next();
}

return proc_result;
';