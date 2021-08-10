package SharedVariables

import org.apache.spark.sql.SparkSession

object closures {
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder().master("local[*]").appName("App").getOrCreate()
    val sc = spark.sparkContext
    var counter = 0;
    val rdd = sc.parallelize(Array(1,2,3,4,5),1)
    rdd.collect()
    rdd.foreach(x => counter += x)
    print(s"Counter : $counter\n");

  }
}
