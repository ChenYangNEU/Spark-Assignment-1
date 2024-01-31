import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Problem4 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("findJack")
      .getOrCreate()

    // Specify the path to your CSV file
    val FilePath = "train.csv"

    // read csv file
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(FilePath)



    val possiJack = df.filter(df("Age") > 18 && df("Sex") === "male" && df("Survived") === 0 && df("Age") < 21 && df("Parch") === 0 && df("Pclass") === 3)
    possiJack.show()

    // Stop the SparkSession when done
    spark.stop()
  }
}

