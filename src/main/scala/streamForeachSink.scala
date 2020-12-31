import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.ForeachWriter


object streamForeachSink {
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

    val customWriter = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long) = true

      override def process(value: Row) = {
        println("Name : "  + value.getAs("Name"))
        println("Open : "  + value.getAs("Open"))
        println("Size : " + value.size)
        println("Values as Seq : " + value.toSeq)
      }

      override def close(errorOrNull: Throwable) = {}
    }

    initDF
      .writeStream
      .outputMode("append")
      .foreach(customWriter)
      .start()
      .awaitTermination()

    // Ref : https://docs.databricks.com/spark/latest/structured-streaming/examples.html#write-to-amazon-dynamodb-using-foreach-in-scala-and-python
  }
}