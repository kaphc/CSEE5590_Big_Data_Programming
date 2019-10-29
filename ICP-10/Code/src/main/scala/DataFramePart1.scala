import org.apache.spark.sql.SparkSession

object DataFramePart1 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config("spark.master", "local")
      .getOrCreate()
    val SQLContext = spark.sqlContext

    val suveryDf = SQLContext.read.option("header", true).csv("data/survey.csv")

    val surveyUS = suveryDf.filter("country like 'United%'")
    surveyUS.registerTempTable("us")

    val surveyCan = suveryDf.filter("country like 'Cana%'")
    surveyUS.registerTempTable("canada")

    SQLContext.sql("SElECT Age, Gender, Timestamp " +
      "FROM us JOIN canada ON us.Age = canada.age").show(10)
  }
}



