package caseclass

case class SurveyAggregates(

                          country: String,
                          value: String,
                          statistic: Double,
                          total_asked: Int,
                          statistic_type:String,
                          who_was_asked: String,
                          variable: String,
                          question: String
                          )

case class SurveyAggregates_unweighted(


                             value: String,
                             statistic: Double,
                             variable: String,
                             country: String,
                             total_asked: Int,
                             who_was_asked: String

                           )