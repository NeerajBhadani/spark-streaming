import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object streamCheckpoint {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Check Point")
      .getOrCreate()

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(List(
      StructField("Date", StringType, true),
      StructField("Open", DoubleType, true),
      StructField("High", DoubleType, true),
      StructField("Low", DoubleType, true),
      StructField("Close", DoubleType, true),
      StructField("Adjusted Close", DoubleType, true),
      StructField("Volume", DoubleType, true)
    ))

    // Extract the Name of the stock from the file name. e.g file names : AMZN_2016.csv, AMZN_2017.csv etc.
    def getFileName : Column = {
      val file_name = reverse(split(input_file_name(), "/")).getItem(0)
      split(file_name, "_").getItem(0)
    }

    // Create Streaming DataFrame by reading data from socket.
    val initDF = (spark
      .readStream
      .option("maxFilesPerTrigger", 1) // This will read maximum of 1 files per mini batch. However, it can read less than 2 files.
      .option("header", true)
      .schema(schema)
      .csv("data/stream")
      .withColumn("Name", getFileName)
      )

//     Some aggregations transformation.
    val resultDF = initDF.select("Name","Date", "Open", "High", "Low")
      .groupBy(col("Name"), year(col("Date")).as("Year"))
      .agg(avg("High").as("Avg"))
        .withColumn("timestamp", current_timestamp())

    resultDF
      .writeStream
      .outputMode("complete") // Try with "update" mode as well.
      .option("checkpointLocation", "checkpoint")
      .format("console")
      .start()
      .awaitTermination()
  }
}