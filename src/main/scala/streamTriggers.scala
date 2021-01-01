import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object streamTriggers {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Trigger")
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
      .option("maxFilesPerTrigger", 1) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
      .option("header", true)
      .schema(schema)
      .csv("data/stream")
      .withColumn("Name", getFileName)
      .withColumn("timestamp", current_timestamp())
      )

    // Aggregation on streaming DataFrame.
    val resultDF = initDF
      .groupBy(col("Name"), year(col("Date")).as("Year"))
      .agg(max("High").as("Max"),
        max("timestamp").as("timestamp"))
      .orderBy(col("timestamp").desc)

    // Using SparkSQL.
    //    initDF.createOrReplaceTempView("initDF")
    //    val query = """select Name, year(date) as year, max(High) as Max from initDF group by Name, Year"""
    //    val stockDf = spark.sql(query)

  /*
    default: If we don't specify trigger then query will be executed in micro-batch mode.
    In micro-batch, next batch will be executed as soon as previous one finished.

    Fixed-Interval mode: processing will be trigger after specified interval.
    If the processing time of previous batch is more than specified interval, next batch will be executed immediately.
    For e.g., If we set processing time as 1 minute, then if micro batch takes 35 sec then it will wait for more 25 sec
    before triggering next batch. If micro batch takes 70 secs, then next will be executed immediately.

    One-Time Micro Batch : All data will be processed in a single micro-batch and application will be terminated.
     */
    resultDF
      .writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("1 minute")) //Trigger.Once(), Trigger.ProcessingTime("1 minute")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }
}
