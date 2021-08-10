package SharedVariables

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}

object sharedVariable {
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder().master("local[*]").appName("App").getOrCreate()
    val sc = spark.sparkContext
    val accum = sc.longAccumulator("My Accumulator")
    val data = sc.parallelize(Array(1,2,3))
    data.foreach(x=>accum.add(x))
    print("vishal rana")
    print(accum.value)

  }
}
