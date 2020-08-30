package SamplePractice

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split,lower}

object Practice {

  def wordCount(sparkSession: SparkSession): Unit ={

    //var rdd=sparkSession.read.text("word")
    var rdd= sparkSession.read.text("wordcount")
      .withColumn("words",split(col("value"),"[^A-Za-z0-9]"))
      //.flatMap(col("words"))
      .select(explode(col("words")).as("wordList"))
      .withColumn("wordsList",lower(col("wordList")))
      .where("wordsList!=' '")
      .groupBy("wordsList").count()//.as("wordcount")
      .select("wordsList","count")
      .show(1000)
   // rdd.collect().foreach(print)

//    var rdd2=rdd//.map(s=>s.toString())
//      .flatMap(s=>s.toString().split(","))
//      .flatMap(s=>s.split(" "))
//      .flatMap(s=>s.split("_"))
//
//    var rdd3=rdd.map(s=>(s,1)).reduceByKey((x,y)=>x+y)
//
//    rdd3.collect().foreach(print);

  }
}
