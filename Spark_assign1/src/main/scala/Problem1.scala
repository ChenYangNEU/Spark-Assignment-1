import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Problem1 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Avg Fares")
      .getOrCreate()

    // Specify the path to your CSV file
    val FilePath = "train.csv"

    // read csv file
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(FilePath)


    val avgFaresByClass: DataFrame = df.groupBy("Pclass")
      .agg(avg("Fare").alias("average_fares"))

    avgFaresByClass.show()

    spark.stop()
  }
}

