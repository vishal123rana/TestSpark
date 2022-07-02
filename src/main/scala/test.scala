

class Data(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)
import org.apache.avro.mapred.AvroJob
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
object test {
  def main(args: Array[String]): Unit = {
    println("vishal rana")

    val spark = SparkSession.builder().master("local[4]").appName("Application").getOrCreate()
    import spark.implicits._
//    val df = spark.read.option("inferSchema", "true").option("header", "true").csv("C:/Users/vishal rana/Desktop/SparkData/data/flight-data/csv/*.csv").as[Data]
//    df.show(false)

    val schema = new StructType()
      .add("DEST_COUNTRY_NAME", StringType)
      .add("ORIGIN_COUNTRY_NAME", StringType)
      .add("count", IntegerType)


    // val bad = spark.createDataset(spark.sparkContext.emptyRDD[Data])
    //    val bad = spark.createDataset(Seq.empty[Data])
    ////    val booksDs = Seq(Data("vishal","rana",3)).toDS()
    ////    booksDs.show()
    ////    val df1 = bad.union(Seq(Data("vishal","rana",3)).toDS())
    ////    df1.show()
    //    var mylist = ListBuffer[Data]()
    //    df.foreach(r => {
    //      if(r.DEST_COUNTRY_NAME == "United States") {
    //        println(r.DEST_COUNTRY_NAME)
    //        println(r.ORIGIN_COUNTRY_NAME)
    //        println(r.count)
    //        mylist += Data(r.DEST_COUNTRY_NAME,r.ORIGIN_COUNTRY_NAME,r.count)
    //      }
    //    })
    //    print(mylist)
    //    df.printSchema
    //    bad.show()
  }
}
