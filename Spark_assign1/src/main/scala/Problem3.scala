import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Problem3 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("findRose")
      .getOrCreate()

    // Specify the path to your CSV file
    val FilePath = "train.csv"

    // read csv file
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(FilePath)

    val possiRose = df.filter(df("Age") === 17 && df("Sex") === "female" && df("Pclass") === 1 && df("Parch") === 1)

    possiRose.show()

    spark.stop()
  }
}
