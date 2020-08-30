package filereader

import caseclass.{SurveyAggregates, SurveyAggregates_unweighted}
import org.apache.spark.sql.{Dataset, SparkSession}

object FileReader {

  def loadFileToDS(sparkSession: SparkSession,fileName:String): Dataset[SurveyAggregates]={

    import sparkSession.sqlContext.implicits._

    return sparkSession.read
      .option("header","true")
      .option("inferSchema",true)
      .csv(fileName)
      .as[SurveyAggregates];


  }

  def loadFileToDS_unweighted(sparkSession: SparkSession,fileName:String): Dataset[SurveyAggregates_unweighted]={

    import sparkSession.sqlContext.implicits._

    return sparkSession.read
      .option("header","true")
      .option("inferSchema",true)
      .csv(fileName)
      .as[SurveyAggregates_unweighted];


  }
}
