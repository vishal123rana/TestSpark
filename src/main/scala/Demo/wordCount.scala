package Demo

import org.apache.spark.sql.SparkSession

object wordCount {
    def main(args:Array[String]):Unit={
      val spark:SparkSession = SparkSession.builder().master("local[*]").appName("App").getOrCreate()
      val sc = spark.sparkContext
     // val df = spark.read.option("header","false").schema(schema).csv("C:/Users/vishal rana/Desktop/data/uber.csv")
      val pairRDD = sc.textFile("C:/Users/vishal rana/Desktop/data/testing.txt")
      pairRDD.flatMap(x=> x.split(" ")).map(word=>(word,1)).reduceByKey((x,y)=>x + y).foreach(println)
    }
}
