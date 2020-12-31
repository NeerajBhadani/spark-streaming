import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object streamForeachBatchSink {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("File Sink")
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
    def getFileName: Column = {
      val file_name = reverse(split(input_file_name(), "/")).getItem(0)
      split(file_name, "_").getItem(0)
    }

    // Create Streaming DataFrame by reading data from socket.
    val initDF = (spark
      .readStream
      .option("maxFilesPerTrigger", 2) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
      .option("header", true)
      .schema(schema)
      .csv("data/stream")
      .withColumn("Name", getFileName)
      )

    val resultDf = initDF.select("Name", "Date", "open")

    def saveToMySql = (df: Dataset[Row], batchId: Long) => {
      val url = """jdbc:mysql://localhost:3306/training"""

      df
          .withColumn("batchId", lit(batchId))
          .write.format("jdbc")
          .option("url", url)
          .option("dbtable", "test")
          .option("user", "root")
          .option("password", "mysql@123")
          .mode("append")
          .save()
  }

    resultDf
      .writeStream
      .outputMode("append")
      .foreachBatch(saveToMySql)
      .start()
      .awaitTermination()
  }
}