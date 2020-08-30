package executors

import caseclass.SurveyAggregates
import filereader.FileReader
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

object SurveyAnalysis {

  def execution(sparkSession: SparkSession): Unit =
  {
      print("Begining Execution!")
    var dsArr:Array[Dataset[SurveyAggregates]]=new Array[Dataset[SurveyAggregates]](2);
      var fob_winter_2018=FileReader.loadFileToDS(sparkSession,"fob_winter_2018_aggregates.csv")
    dsArr(0)=fob_winter_2018;
    var fob_spring_2019=FileReader.loadFileToDS(sparkSession,"fob_spring_2019_aggregates_unweighted.csv")
    dsArr(1)=fob_spring_2019;
    var fob_firsthalf_2020=FileReader.loadFileToDS_unweighted(sparkSession,"fob_0120_a_data_aggregate_unweighted_smb.csv")
    var fob_covidfirstwave_2020=FileReader.loadFileToDS_unweighted(sparkSession,"fob_0219_data_aggregate_unweighted_smb.csv")
    var fob_covid_secondwave_2020=FileReader.loadFileToDS_unweighted(sparkSession,"fob_0120_b_data_aggregate_unweighted_smb.csv")


    dsArr.foreach(s=>s.show(2000));
//    fob_firsthalf_2020.show(1000)
//    fob_covidfirstwave_2020.show(1000)
//    fob_covid_secondwave_2020.show(1000)

      findCountofNewBusinessAcrossCountry(fob_winter_2018.union(fob_spring_2019));
  }

  def findCountofNewBusinessAcrossCountry(ds: Dataset[SurveyAggregates]) : Unit ={

    var result=ds
        //.where(col("value")=="1 to 3 years" && col("value")="")
      .where("value='1 to 3 years' or value='Less than 1 year'")
      .groupBy("country")
      .sum("statistic").withColumnRenamed("sum(statistic)","total_new_business")
        .select("country","total_new_business")
        .orderBy("country")
        

    result.show(1000);
  }
}
