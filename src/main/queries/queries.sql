CREATE EXTERNAL TABLE kakfa_streaming_customer_data
  (`customer_id` INT , `attribute_name` string,  `attribute_value` string)
  STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
  TBLPROPERTIES ("kafka.topic" = "customer", "kafka.bootstrap.servers"="localhost:9092");
