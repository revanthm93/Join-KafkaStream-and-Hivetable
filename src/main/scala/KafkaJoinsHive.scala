import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import sparkSession.implicits._
import sparkSession.sql
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._

class KafkaJoinsHive {

  case class Transaction(customer_id: Int, attribute_name1: String, attribute_value: String)

  def consumeFromKafka(ssc, topic: String): Any = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "cg",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }

  def ReadfromHive(spark, table: String): Any = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(spark.SparkContext)
    val test_enc_orc = hiveContext.sql("select * from %s", table)
    test_enc_orc
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Kafka&HiveJoin").config("hive.metastore.warehouse.dir", hiveHost + "user/hive/warehouse").enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.SparkContext, Seconds(100000))
    val kafka_streaming_data = consumeFromKafka(ssc, "cust_info")
    val hive_static_data = ReadfromHive(spark, "email_mappings")
    val hive_static_data_rdd = hive_static_data.rdd
    val structured_kafka_streaming_data = kafka_streaming_data.map(x => (Transaction(x.customer_id, x.attribute_name, x.attribute_value)))
    val joined_df = structured_kafka_streaming_data.transform(kafka_streaming_data => kafka_streaming_data.join(hive_static_data_rdd))
    val pivot_df = joined_df.groupBy("customer_id").pivot("attribute_name").agg(collect_list("attribute_value"))
    pivot_df.coalesce(1).write.mode("append").format("com.intelli.spark.csv").option("header", "true").save("out.csv")
    ssc.start()
    ssc.awaitTermination()
  }
}