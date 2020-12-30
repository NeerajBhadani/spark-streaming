import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object streamRateSource {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Rate Source")
      .getOrCreate()

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    // Create Streaming DataFrame by reading data from socket.
    val initDF = (spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      )
    // Check if DataFrame is streaming or Not.
    println("Streaming DataFrame : " + initDF.isStreaming)

    // Print Schema of DataFrame
    println("Schema : " + initDF.printSchema())

    val resultDF = initDF
        .withColumn("result", col("value") + lit(1))

    resultDF
      .writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()
  }
}