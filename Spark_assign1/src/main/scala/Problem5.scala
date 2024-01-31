import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, _}

object Problem5 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("separate based on age")
      .getOrCreate()

    // Specify the path to your CSV file
    val FilePath = "train.csv"

    // read csv file
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(FilePath)

    val dfwithAgeRange = df.withColumn("AgeGroup", ((col("Age") - 1)/ 10 + 1).cast("int"))

    val AgeGroupFare = dfwithAgeRange.groupBy("AgeGroup").agg(avg("Fare").alias("avg Fare")).orderBy("AgeGroup")
    AgeGroupFare.show()

    val AgeGroupSurvived = dfwithAgeRange.groupBy("AgeGroup")
      .agg(count(when(col("Survived")=== 1,1)).alias("alive"), count("*").alias("Total"))
      .withColumn("Percentage", col("alive")/col("Total"))
      .orderBy(desc("Percentage"))
    println("~~~~~~~~~~~~~~~~~~~~~~~")

    AgeGroupSurvived.show()

    spark.stop()
  }
}
