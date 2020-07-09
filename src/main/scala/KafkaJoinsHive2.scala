[After creating table]

import xyz
spark and hive context

  val streaming_table = hiveContext.sql("SELECT COUNT(*) FROM kakfa_streaming_customer_data
  WHERE `__timestamp` > 1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval ' 30 ' MINUTES");

  val static_hive_email_table = hiveContext.sql ("select * from email_mappings") )

  val joined_df = static_hive_email_table.join (streaming_table, static_hive_email_table[customer_id] == streaming_table[customer_id] )
  val pivot_df = joined_df.groupBy ("customer_id").pivot ("attribute_name").agg (collect_list ("attribute_value") )

  pivot_df.coalesce(1).write.mode ("append").format ("com.intelli.spark.csv").option ("header", "true").save ("out.csv")
