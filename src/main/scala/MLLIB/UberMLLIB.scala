package MLLIB

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object UberMLLIB {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("App").getOrCreate()
    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("base", StringType, true)
    ))
    val df = spark.read.option("header", "false").schema(schema).csv("C:/Users/vishal rana/Desktop/data/uber.csv")
    // df.printSchema()
    df.explain(true)

    df.show()
    //     df.cache()
    //     // df.show()
    val featureCols = Array("lat", "lon")

    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    //it helps concatenate all your feature into big Vector
    val df2 = assembler.transform(df)
    //transform
    df2.show()
    val Array(train, test) = df2.randomSplit(Array(0.7, 0.3), 5043)

    //  print(test)
    // currently 5 cluster before 5 value 8 cluster
    val kmeans = new KMeans().setK(5).setFeaturesCol("features").setPredictionCol("Prediction")
    val model = kmeans.fit(train)
    println("Final Centers:")
    model.clusterCenters.foreach(println)
    val categories = model.transform(test)
    categories.sort("Prediction").where("Prediction = 1").show()
    categories.groupBy("Prediction").count().sort("Prediction").show() //How many pickups occurred in each cluster?

    ///     print(model.computeCost(test))
    //     model.write.overwrite().save("C:/Users/vishal rana/Desktop/spark-ml-kmeans-uber-master/data/savemodel")//save model
    categories.createOrReplaceTempView("uber")
    //  categories.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"prediction").groupBy("month", "day", "hour", "prediction").agg(functions.count("prediction").alias("count")).orderBy("day", "hour", "prediction").show
    spark.sql("select * from uber")
  }
}
