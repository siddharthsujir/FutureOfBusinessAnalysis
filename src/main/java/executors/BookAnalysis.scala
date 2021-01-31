package executors

import caseclass.BookRating
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{dense_rank, desc, rank,length,col,split,explode};

object BookAnalysis {


  def bookAnalysis(): Unit={

    var sparkSession= SparkSession.builder()
      .master("local[*]")
      .appName("BookSellAnalysis")
      .getOrCreate()

//    var bookRatingDS=readFile(sparkSession);
    ////
    ////    bookRatingDS.show(100);
    ////
    ////    printRankDenseRank(bookRatingDS)
    ////
    ////    val s=printRankDenseRank(bookRatingDS);
    ////    s.write.csv("C:\\Users\\siddhu\\Documents\\output\\bookanalysis")

    val s=readFileAsText(sparkSession)
    var wc=s.filter(length(col("value"))>0)
      .withColumn("words",split(col("value"),"\\s+"))
      .select(explode(col("words")) as "word")
      .groupBy("word")
      .count()
      .orderBy(col("count").desc)

    wc.show()
  }

  def printRankDenseRank(ds:Dataset[BookRating]): DataFrame={

//    ds.sqlContext.sql("Select Name, " +
//      "Author," +
//      "UserRating," +
//      "Reviews," +
//      "Genre, " +
//      "rank() over(partition by Genre order by UserRating)," +
//      "dense_rank() over(partition by Genre order by UserRating)").show(100)

    var w=Window.partitionBy("Genre").orderBy(desc("UserRating"))



    ds.withColumn("Rank_func",rank().over(w))
    .withColumn("Dense_Rank",dense_rank().over(w))
    //.show(1000);

  }

  def readFile(sparkSession: SparkSession): Dataset[BookRating]={

    import sparkSession.sqlContext.implicits._;
    sparkSession.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\siddhu\\Downloads\\bestsellers_with_categories.csv")
      .as[BookRating]

  }

  def readFileAsText(sparkSession: SparkSession):Dataset[String] = {

    import sparkSession.sqlContext.implicits._;
    sparkSession.read.format("text")
      .option("header","true")
      .load("C:\\Users\\siddhu\\Downloads\\bestsellers_with_categories.csv")
      .as[String]
  }
}
