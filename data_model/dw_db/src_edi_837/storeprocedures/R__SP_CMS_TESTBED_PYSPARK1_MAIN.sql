USE SCHEMA SRC_EDI_837;

CREATE OR REPLACE PROCEDURE SP_CMS_TESTBED_PYSPARK1_MAIN("STAGE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.dataframe_reader import *
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import snowflake.snowpark.functions as f
import logging
input_path = "/datalake/optum/optuminsight/p_dlz/prd/EDP_Cons/p_hdfs/uhc/processed/standard_access/edp/data/CMS_837P_O/CMS_837P_O.v0/CMS_837P_O_2017122900*.txt"
    #input_path = sys.argv[1]
    #output_path = sys.argv[2]
# Log Function
def logFunction():
    # Set the standard log prefix
    formatString = ''%(asctime)s Function:%(funcName)s(); %(message)s''
    #formatString = ''%(asctime)s %(filename)s Function:%(funcName)s() %(message)s''
    # Set the log level - to logging.INFO and above
    logging.basicConfig(level=logging.INFO, format=formatString)
    # Get a logger object with this module name
    logger = logging.getLogger(__name__)
    return logger
# Start logging
logger = logFunction()
logger.info("INFO: RUNNING CMS TESTBED Process PART I Script")
def main(Session: snowpark.Session,stage_name:str): 
# Create a SparkSession
   #connection_parameters = { "account": "",
   ##"user": "<your snowflake user>",
   ##"password": "<your snowflake password>",
   ##"role": "<your snowflake role>",  # optional
   ##"warehouse": "<your snowflake warehouse>",  # optional
   ##"database": "<your snowflake database>",  # optional
   ##"schema": "<your snowflake schema>",  # optional
    #                       }
#
    #Session = Session.builder.configs(connection_parameters).create()  
# Load the data and Create Dataframe
    #prof_raw = Session.read.csv("@UTIL.STAGE_AZURE_ISDC/landing/testbed/")
    prof_raw = Session.sql(stage_name)
    # Removing Special Characters from the data.
    clm_raw_rep = prof_raw.withColumn("line1", regexp_replace(col("value"), "[!%;`]", "")).drop(col("value"))
    #clm_raw_rep_concatenation = clm_raw_rep.withColumn("line1", concat_ws(lit("") , listagg(clm_raw_rep.line1)))
    # Splitting a row into Multiple Rows
    prof_segmentized = clm_raw_rep.withColumn(''l1'',regexp_replace(col("line1"),lit("~"),lit("\\n")))
    prof_segmentized1=prof_segmentized.join_table_function("split_to_table",prof_segmentized["l1"],lit("\\n"))
    prof_segmentized2=prof_segmentized1.select(prof_segmentized1.value).with_column_renamed("value","l1")
    prof_submitter_delimited = prof_segmentized2.selectExpr(
                 """CASE
                WHEN l1 rlike ''^ST.(.*)'' THEN ''^~''
                WHEN l1 rlike ''^HL.(.*)22.*'' THEN ''!~''
                WHEN l1 rlike ''^HL.(.*)20.*'' THEN ''`~''
                WHEN l1 rlike ''^CLM.*'' THEN ''||~''
                WHEN l1 rlike ''^SE.(.*)'' THEN ''%~''
                ELSE ''~''
        END AS delimiter""",
        "l1"
        )
    prof_submitter_delimited_mergeColumns = prof_submitter_delimited.withColumn("columnMerged", concat_ws(lit(""), col("delimiter"), col("l1"))).drop("l1", "delimiter")
    prof_submitter_delimited_N = prof_submitter_delimited_mergeColumns.withColumnRenamed("columnMerged", "l1")
    prof_submitter_delimited_con = prof_submitter_delimited_N.withColumn("l1", concat_ws(lit(""), listagg(prof_submitter_delimited_N.l1)))
    prof_submitter_records_tuple_inter = prof_submitter_delimited_con.withColumn(''lA_Inter_Col'',split(''l1'',lit(''^''))).drop(col("l1"))
    prof_submitter_records_tuple = prof_submitter_records_tuple_inter.withColumn("lA", explode_outer(prof_submitter_records_tuple_inter.lA_Inter_Col)).drop(''lA_Inter_Col'')
    clm_tokenize_hl_prv = prof_submitter_records_tuple.withColumn(''bag1'', split(''lA'', lit(''`'')))
    clm_flatten_hl_prv_1 = clm_tokenize_hl_prv.selectExpr("bag1[0] as col0", "bag1")
    clm_flatten_hl_prv_2 = clm_flatten_hl_prv_1.withColumn( "col1",explode_outer(clm_flatten_hl_prv_1.bag1))
    clm_flatten_hl_sbr_3 = clm_flatten_hl_prv_2.selectExpr("col0", "col1")
    clm_tokenize_hl_sbr = clm_flatten_hl_sbr_3.withColumn("bag2", split(''col1'',lit(''!'')))
    clm_tokenize_hl_sbr1 = clm_tokenize_hl_sbr.selectExpr("col0","bag2")
    clm_flatten_hl_sbr_1 = clm_tokenize_hl_sbr1.selectExpr("col0", "bag2[0] as col1", "bag2")
    clm_flatten_hl_sbr_2 = clm_flatten_hl_sbr_1.withColumn("col2", explode_outer(clm_flatten_hl_sbr_1.bag2))
    clm_flatten_hl_sbr_3 = clm_flatten_hl_sbr_2.selectExpr("col0", "col1", "col2")
    #clm_filter_hl_sbr = clm_flatten_hl_sbr_3.filter(col("col2").rlike(".*HL.(.*)22.*"))
    clm_filter_hl_sbr = clm_flatten_hl_sbr_3.filter(col("col2").rlike(".*HL\\\\*([0-9]*)\\\\*([0-9]*)\\\\*22.*"))
    clm_tokenize_clm_inter1 = clm_filter_hl_sbr.withColumn("bag3", split(clm_filter_hl_sbr.col2, lit(''||''))) 
    clm_tokenize_clm_inter2 = clm_tokenize_clm_inter1.selectExpr("col0","col1", "bag3") 
    clm_tokenize_clm = clm_tokenize_clm_inter2.withColumn("bag3",expr(''array_except(bag3,[])''))
    clm_flatten_clm_1 = clm_tokenize_clm.selectExpr("col0","col1", "bag3[0] as col2", "bag3")
    clm_flatten_clm_2_1 = clm_flatten_clm_1.withColumn( "col3", explode_outer(clm_flatten_clm_1.bag3))
    clm_flatten_clm_2 = clm_flatten_clm_2_1.selectExpr("col0","col1", "col2", "col3")
    clm_tokenize_se1 = clm_flatten_clm_2.withColumn("bag4", split(''col3'', lit(''%'')))
    clm_tokenize_se = clm_tokenize_se1.selectExpr("col0","col1", "col2", "bag4")
    clm_flatten_se_1 = clm_tokenize_se.selectExpr("col0", "col1",  "col2", "bag4[0] as col3", "bag4")
    clm_flatten_se_2 = clm_flatten_se_1.withColumn( "col4", explode_outer(clm_flatten_se_1.bag4))
    clm_flatten_se_3 = clm_flatten_se_2.selectExpr("col0", "col1", "col2", "col3", "col4")
    prof_filtered_hl22_only = clm_flatten_se_3.filter(col("col0").rlike("^(~ST.).*"))
    # Splitting col0 column by * Delimiter & appending the elements at the end of the dataframe. col_list will have list of columns (col4, col5, col6 and soon)
    clm_records_split_1 = prof_filtered_hl22_only.withColumn( "col_list", split(''col0'',lit(''*'')))
    clm_records_split = clm_records_split_1.selectExpr("col0", "col1", "col2", "col3", "col_list")
    clm_records_split_new = clm_records_split.withColumn("col1", regexp_replace("col1", "~HL\\\\*([0-9]*)\\\\*([0-9]*)\\\\*20*", "~HL*1*20*")).withColumn("col2", regexp_replace("col2", "~HL\\\\*([0-9]*)\\\\*([0-9]*)\\\\*22*", "~HL*2*1*22*"))
    sample1 = clm_records_split_new.selectExpr(
        "col0",
        "col1",
        "col2",
        "col3",
        "col_list[0] as col4",
        "col_list[1] as col5",
        "col_list[2] as col6"
    )
    clm_raw_rep_se = prof_raw.withColumn("line1", regexp_replace(col("value"), "", "")).drop(col("value"))
    prof_raw_se_concatenation = clm_raw_rep_se.withColumn("line1", concat_ws(lit(""), listagg(clm_raw_rep_se.line1)))
    prof_segmentized_se_split = prof_raw_se_concatenation.withColumn(''l1'',split(''line1'',lit("~"))).drop("line1")
    prof_segmentized_se = prof_segmentized_se_split.withColumn(''l2'',explode(prof_segmentized_se_split.l1)).drop("l1")
    #CHECKPOINT6: Writing Output to File System -- Matched output
    #prof_segmentized_se.write.option("delimiter", "@").csv(output_path+''_checkpoint6'', header=False, mode="overwrite")
 
    # We only wants to select Segments which starts with SE.
    clm_segments = prof_raw.filter(col("value").rlike("^(SE.).*"))   ## It should be SE
    #prof_strsplit_se = clm_segments.selectExpr("l1 as c0", "Split(l1,''[*]'')[0] as c1", "Split(l1,''[*]'')[1] as c2", "Split(l1,''[*]'')[2] as c3", "Split(l1,''[*]'')[3] as c4")
    prof_strsplit_se_inter1 = clm_segments.withColumn("col_list", split(''value'',lit(''*'')))
    prof_strsplit_se_inter = prof_strsplit_se_inter1.selectExpr("value as prof_col0", "col_list")
    prof_strsplit_se = prof_strsplit_se_inter.selectExpr("prof_col0", "col_list[0] as prof_col1", "col_list[1] as prof_col2", "col_list[2] as prof_col3")
    #CHECKPOINT7: Writing Output to File System
    
    # join trailer to header and body of the transaction set
    prof_st_se_joined = sample1.join(prof_strsplit_se, sample1["col6"] == prof_strsplit_se["prof_col3"])
    prof_selected = prof_st_se_joined.selectExpr("col0", "col1", "col2", "col3", "''~''", "prof_col0")
    prof_selected.write.mode("append").save_as_table("PROF_STRSPLIT_SE")
    #return prof_raw
    #return prof_submitter_delimited
    #return prof_selected
    
    
    #return prof_selected
    ';