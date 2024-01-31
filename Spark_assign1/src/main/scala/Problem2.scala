import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Problem2 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SurvivalRate")
      .getOrCreate()

    // Specify the path to your CSV file
    val FilePath = "train.csv"

    // read csv file
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(FilePath)

    val dfSurvivedbyP = df.groupBy("Pclass").
      agg(count(when(col("Survived")=== 1,1)).alias("alive"), count("*").alias("Total")).
      withColumn("Percentage", col("alive")/col("Total"))

    dfSurvivedbyP.show()

    val highestClass = dfSurvivedbyP.orderBy(desc("Percentage")).select(col("Pclass")).first().getAs[Int](0)
    println("The highest rate of the class is: " + highestClass)

    spark.stop()
  }
}

