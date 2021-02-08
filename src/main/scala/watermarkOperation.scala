import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object watermarkOperation {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("WaterMark")
      .getOrCreate()

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    // Define host and port number to Listen.
    val host = "127.0.0.1"
    val port = "9999"

    // Create Streaming DataFrame by reading data from socket.
    val initDF = spark
      .readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    // Create DataFrame  with event_timestamp and val column
    val eventDF = initDF.select(split(col("value"), "#").as("data"))
      .withColumn("event_timestamp", element_at(col("data"),1).cast("timestamp"))
      .withColumn("val", element_at(col("data"),2).cast("int"))
      .drop("data")

    // Without Watermarking
    //    val resultDF = eventDF
    //      .groupBy(window(col("event_timestamp"), "5 minute"))
    //      .agg(sum("val").as("sum"))

    val resultDF = eventDF
      .withWatermark("event_timestamp", "10 minutes")
      .groupBy(window(col("event_timestamp"), "5 minute"))
      .agg(sum("val").as("sum"))

    // Display DataFrame on console.
    resultDF
      .writeStream
      .outputMode("update")
      .option("truncate", false)
      .option("numRows", 10)
      .format("console")
      .start()
      .awaitTermination()
  }
}