USE SCHEMA SRC_EDI_837;
CREATE OR REPLACE PROCEDURE "SP_PROVIDER_SPIKE_PIVOT_PREV_RESULTS"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var cols_query = `
      select ''\\\\'''' 
        || listagg(distinct pivot_column, ''\\\\'',\\\\'''') within group (order by pivot_column desc)
        || ''\\\\'''' 
      from table(result_scan(last_query_id(-1)))`;
  var stmt1 = snowflake.createStatement({sqlText: cols_query});
  var results1 = stmt1.execute();
  results1.next();
  var col_list = results1.getColumnValue(1);
  
  pivot_query = `
         select * 
         from (select * from table(result_scan(last_query_id(-2)))) 
         pivot(max(pivot_value) for pivot_column in (${col_list}))
     `
  var stmt2 = snowflake.createStatement({sqlText: pivot_query});
  stmt2.execute();
  return `select * from table(result_scan(''${stmt2.getQueryId()}''));\\n  select * from table(result_scan(last_query_id(-2)));`;
';

