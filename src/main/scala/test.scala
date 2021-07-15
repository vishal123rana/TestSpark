import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    println("vishal rana")
    val spark = SparkSession.builder().master("local[4]").appName("Application").getOrCreate()
    val df = spark.read.option("inferSchema","true").option("header","true").csv("C:/Users/vishal rana/Desktop/SparkData/data/flight-data/csv/*.csv")
    val new_rdd = df.rdd;
  //  new_rdd.checkpoint()
    new_rdd.foreach(println)
    println(new_rdd.dependencies)
  }
}
