import SamplePractice.Practice
import executors.{BookAnalysis, SurveyAnalysis}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BusinessEngine extends App {

 override def main(args: Array[String]): Unit= {

    print("Hello! Welcome to the Future of Business survey analysis!");

//    var conf=new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("future_of_Business")
//
//    var sparkSession=SparkSession
//      .builder()
//      .config(conf)
//      .getOrCreate();

    //SurveyAnalysis.execution(sparkSession);
   //Practice.wordCount_ByLine(sparkSession)
   BookAnalysis.bookAnalysis();
  }

}
