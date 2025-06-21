import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("My Spark").master("local[*]").getOrCreate()

    val sales = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val sales1=sales
    //sales1.write.parquet("C:/Users/gomall/Desktop/resultDataSetParquet.parquet")
    sales1.write.csv("C:/Users/gomall/Desktop/result1.csv")


  }

}