import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object streamUdf {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("File Source")
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
      .option("maxFilesPerTrigger", 2) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
      .option("header", true)
      .schema(schema)
      .csv("data/stream")
      .withColumn("Name", getFileName)
        .select("Name", "Date","Open", "High", "Low", "Close", "Adjusted Close", "Volume")
      )

    val ex = expr("IF((Close - Open) > 0, 'UP', 'DOWN')")

    def up = (close:Double, open:Double) => {
      if ((close - open) > 0)
        "UP"
      else
        "Down"
    }

    // Register up function as UDF.
    val upUdf = udf(up)

    // Display Data to Console with Aggregation using "update" and "complete" mode.
//    val resultDF = initDF
//      .withColumn("up_down_udf", upUdf(col("Close"), col("Open")))
//      .select("Name", "Date", "Open", "Close","up_down_udf")
//      .withColumn("up_down_expr", ex)

    // Using SparkSQL
    // Register UDF
    spark.udf.register("up_down", up)

    // Create Temp View
    initDF.createOrReplaceTempView("initDF")

    // Apply UDF in SQL query.
    val resultDF = spark.sql("select *, up_down(Close, Open) as up_down_udf from initDF")

    resultDF
      .writeStream
      .outputMode("append")
      .option("truncate", false)
      .option("numRows", 5)
      .format("console")
      .start()
      .awaitTermination()
  }
}
