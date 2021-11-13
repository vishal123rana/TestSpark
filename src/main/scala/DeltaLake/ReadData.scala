package DeltaLake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object ReadData {
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").appName("App").getOrCreate()
   // val df = spark.read.format("delta").load("/tmp/delta-table")
  ////  df.show()
//    val data1 = spark.range(5, 10)
////    data1.write.format("delta").mode("overwrite").save("/tmp/delta-table")
//    val df = spark.read.format("delta").option("timestampAsOf", "2019-01-01").load("/tmp/delta-table")
//    df.show
  val deltaTable = DeltaTable.forPath("/tmp/delta-table")

    // Update every even value by adding 100 to it
    deltaTable.update(
      condition = expr("id % 2 == 0"),
      set = Map("id" -> expr("id + 100")))

    // Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))

    // Upsert (merge) new data
    val newData = spark.range(0, 20).toDF

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

    deltaTable.toDF.show()
  }
}
