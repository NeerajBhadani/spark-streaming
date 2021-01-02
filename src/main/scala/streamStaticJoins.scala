import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

// Ref : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#join-operations

object streamStaticJoins {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()

    import spark.implicits._

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
      .option("maxFilesPerTrigger", 2) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
      .option("header", true)
      .schema(schema)
      .csv("data/stream")
      .withColumn("Name", getFileName)
      )

   val resultDF = initDF.groupBy($"Name", year($"Date").as("Year"))
          .agg(max("High").as("Max"))

    // Read static Data
    val companyDF = spark.read.option("header", true)
                      .csv("data/stocks/COMPANY.csv")
    companyDF.show()

    // Check if DataFrame is streaming or Not.
    println("resultDF Streaming DataFrame : " + resultDF.isStreaming)
    println("companyDF Streaming DataFrame : " + companyDF.isStreaming)

    // Static - Stream Joins
    // Inner join
//    val joinDf = resultDF.join(companyDF, Seq("Name"))

    // Left-outer Join : Stream - Static left outer join will work.
    /* Here we are matching all the records from Stream DataFrame on Left with Static DataFrame on Right.
    If records are not match from Stream DF (Left) to Static DF (Right) then NULL will be returned,
    since the data for Static DF will not change.
     */
//    val joinDf = resultDF.join(companyDF, Seq("Name"), "left_outer")

    // Left-outer Join : Static - Stream left outer join will not work.
    /* Here we are matching all the records from Static DataFrame on Left with Stream DataFrame on Right.
      If records are not match from Static DF (Left) to Stream DF (Right) then we cannot return NULL,
      since the Data is changing on Stream DF (Right) we cannot guarantee if we will get matching records or Not.
       */
    val joinDf = companyDF.join(resultDF, Seq("Name"), "left_outer")

    joinDf
      .writeStream
      .outputMode("complete")
      .option("truncate", false)
      .option("numRows", 10)
      .format("console")
      .start()
      .awaitTermination()
  }
}