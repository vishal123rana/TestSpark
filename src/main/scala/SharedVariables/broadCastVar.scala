package SharedVariables

import org.apache.spark.sql.SparkSession

object broadCastVar {
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder().master("local[*]").appName("App").getOrCreate()
    val sc = spark.sparkContext
    val v = sc.broadcast(Array(1,2,3,4));
    print("vishal rana\n")
    print(v.value)
    for(i <- v.value){
      print(i);
    }

  }
}
