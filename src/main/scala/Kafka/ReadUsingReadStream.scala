package Kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

object ReadUsingReadStream {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Application").getOrCreate()
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.100:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest") // From starting
      .load()
    df.printSchema()
    val personStringDF = df.selectExpr("CAST(value AS STRING)")
    val schema = new StructType()
      .add("home", StringType)
      .add("lat", DoubleType)
      .add("lon", DoubleType)
    val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")
    //    personDF.writeStream
    //      .format("console")
    //      .outputMode("append")
    //      .start()
    //      .awaitTermination()
    personDF.writeStream.format("console").outputMode("append").start()
  }
}
