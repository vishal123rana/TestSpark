
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object sparkAsAConsumer {
  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092";
    val groupid = "grp1";
    val topic = "kafka";
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->brokers,
      ConsumerConfig.GROUP_ID_CONFIG-> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer]
    )

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    val ssc = new StreamingContext(sparkConf,Seconds(1));
    val sc = ssc.sparkContext;
    sc.setLogLevel("OFF")
    val topicSet = topic.split(",").toSet
    val message = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topicSet,kafkaParams))
    val line = message.map(_.value)
    val words = line.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x,1L)).reduceByKey(_+_)
    wordCounts.print();
    ssc.start()
    ssc.awaitTermination()
  }
}
