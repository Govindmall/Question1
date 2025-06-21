import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DistinctCountries {
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    //get distinct countries
    val countriesDF=salesDF.select("Country").distinct()
    //show the number of countries
    val numDistinctCountries=countriesDF.count()
    println(s"The number of countries: $numDistinctCountries")
    //sales1.write.parquet("C:/Users/gomall/Desktop/resultDataSetParquet.parquet")
    countriesDF.write.csv("C:/Users/gomall/Desktop/result1.csv")
    spark.stop()


  }

}