import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object streamSocketSource {

  def main(args: Array[String]) {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Socket Source")
      .getOrCreate()

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    // Define host and port number to Listen.
    val host = "127.0.0.1"
    val port = "9999"

    // Create Streaming DataFrame by reading data from socket.
    val initDF = (spark
      .readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load())

    // Check if DataFrame is streaming or Not.
    println("Streaming DataFrame : " + initDF.isStreaming)

    // Print Schema of DataFrame
    println("Schema of DataFame initDF ...")
    println(initDF.printSchema())

    // Perform word count on streaming DataFrame
    val wordCount = initDF
      .select(explode(split(col("value"), " ")).alias("words"))
      .groupBy("words")
      .count()

    // Print Schema of DataFrame
    println("Schema of DataFame wordCount ...")
    println(wordCount.printSchema())

    // Printout the data to console.
    wordCount
      .writeStream
      .outputMode("complete") // Try "update" and "complete" mode.
      .option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()
  }
}