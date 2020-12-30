import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._

object streamFileSource {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Word Count")
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

    // Create Streaming DataFrame by reading data from File Source.
    val initDF = (spark
      .readStream
      .format("csv")
      .option("maxFilesPerTrigger", 2) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
      .option("header", true)
      .option("path", "data/stream")
      .schema(schema)
      .load()
      .withColumn("Name", getFileName)
      )

    // Check if DataFrame is streaming or Not.
    println("Is this Streaming DataFrame : " + initDF.isStreaming)

    // Print Schema of DataFrame
    println("Schema of DataFame initDF ...")
    println(initDF.printSchema())

    // Display Data to Console without Aggregation using "append" mode.
//    initDF
//      .writeStream
//      .outputMode("append")
//      .option("truncate", false)
//      .option("numRows", 3)
//      .format("console")
//      .start()
//      .awaitTermination()

     // Perform some aggregation on streaming Data.
//     val resultDF = initDF
//              .groupBy(col("Name"), year(col("Date")).as("Year"))
//              .agg(max("High").as("Max"))

    // Using Spark-SQL
    initDF.createOrReplaceTempView("stockView")
    val query = """select year(Date) as Year, Name, max(High) as Max from stockView group by Name, Year"""
    val resultDF = spark.sql(query)

    resultDF
      .writeStream
      .outputMode("update") // Try "update" and "complete" mode.
      .option("truncate", false)
      .option("numRows", 3)
      .format("console")
      .start()
      .awaitTermination()
  }
}