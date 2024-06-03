USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_FEKP_TESTBED()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

var row_count_query=snowflake.createStatement({sqlText: "select count(distinct metadata$filename) as Count from @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/initial/fek/FEK_837PS_O.v0"});

var result_set=row_count_query.execute(); result_set.next();
var Last_Value=result_set.getColumnValue(1);

var row_count_query_2=snowflake.createStatement({sqlText: "select count(doc_ctl_nbr) as Count from SRC_EDI_837.FEKP_TESTBED_DATA"});
var result_set_2=row_count_query_2.execute(); 
result_set_2.next();
var Cntl_Value=result_set_2.getColumnValue(1);


var create_tab_query=snowflake.createStatement({sqlText: "CREATE OR REPLACE TRANSIENT TABLE SRC_EDI_837.FEK_P_FINAL (clm_data VARCHAR, clm_date VARCHAR)"});
var result_set_3=create_tab_query.execute(); 
result_set_3.next();

var create_tab_query_2=snowflake.createStatement({sqlText: "CREATE OR REPLACE TRANSIENT TABLE SRC_EDI_837.FEKP_CLAIM_TEMP (clmdata VARCHAR)"});
var result_set_4=create_tab_query_2.execute(); 
result_set_4.next();

var create_tab_query_3=snowflake.createStatement({sqlText: "CREATE OR REPLACE TRANSIENT TABLE SRC_EDI_837.FEKP_CLAIM (clmdata VARCHAR, clm_date VARCHAR)"});
var result_set_5=create_tab_query_3.execute(); 
result_set_5.next();

var create_tab_query_4=snowflake.createStatement({sqlText: "CREATE OR REPLACE TRANSIENT TABLE SRC_EDI_837.FEK_P_MERGE_FINAL (clmdata VARCHAR, clm_date VARCHAR, loc VARCHAR)"});
var result_set_6=create_tab_query_4.execute(); 
result_set_6.next();



i=0;
j=0;

 for (i=0; i < Last_Value; i++) 
 {

    

        var proc_1 = `select split_part(metadata$filename,''/'',-1) as f_name from @UTIL.STAGE_AZURE_ISDC/pre_clm/inbox/testbed_preprocess/initial/fek/FEK_837PS_O.v0 ORDER BY f_name LIMIT 1 OFFSET ${i}`;
        var statement=snowflake.createStatement({sqlText: proc_1});
        var result= statement.execute(); result.next();
        var param1 =result.getColumnValue(1);

        var proc_main= `call SRC_EDI_837.SP_FEKP_TESTBED_CLAIM_LOAD(''${param1}'')`;
        var proc_statement = snowflake.createStatement({sqlText: proc_main});
        var proc_result= proc_statement.execute(); 
        proc_result.next(); 

        

    
    }


for (j=0; j < Cntl_Value; j++)
    {

    var proc_2 = `select doc_ctl_nbr from SRC_EDI_837.FEKP_TESTBED_DATA ORDER BY doc_ctl_nbr LIMIT 1 OFFSET ${j}`;
        var statement=snowflake.createStatement({sqlText: proc_2});
        var result_2= statement.execute(); result_2.next();
        var param2 =result_2.getColumnValue(1);

        var proc_main= `call SRC_EDI_837.SP_FEKP_TESTBED_MAIN(''${param2}'')`;
        var proc_statement = snowflake.createStatement({sqlText: proc_main});
        var proc_result= proc_statement.execute(); 
        proc_result.next(); 

}

var proc_copy= `call SRC_EDI_837.SP_FEKP_TESTBED_REPORT()`;
var proc_statement = snowflake.createStatement({sqlText: proc_copy});
var proc_result= proc_statement.execute(); 
proc_result.next();


return proc_result;
';