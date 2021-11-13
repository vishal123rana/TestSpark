package DeltaLake
import org.apache.spark.sql.SparkSession

object First {
   def main(arge:Array[String]):Unit={
      val spark:SparkSession = SparkSession.builder().master("local[*]").appName("App").getOrCreate()
      val data = spark.range(0, 5)
      data.write.format("delta").save("/tmp/delta-table")
   }

}
