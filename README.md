# spark-streaming
This repository contains code for Spark Streaming

# Data File
We will use some of the stock data available [here](https://github.com/szrlee/Stock-Time-Series-Analysis/tree/master/data). 
For example, Apple stock data present in this file: [AAPL_2006–01–01_to_2018–01–01.csv](https://github.com/szrlee/Stock-Time-Series-Analysis/blob/master/data/AAPL_2006-01-01_to_2018-01-01.csv).
 We will take the data for a few years like `2015`, `2016`, and `2017` and manually save it to a different file like `AAPL_2015.csv`, `AAPL_2016.csv` and `AAPL_2017.csv` respectively. 
 Similarly, we will create the sample data for `Google`, `Amazon`, and `Microsoft` as well. 
 We will keep all the CSV files locally under `data/stocks` folder. 
 Also, create another folder `data/stream` which we will use to simulate the streaming data.

# Medium Blogs
1. [Apache Spark Structured Streaming - First Streaming Example (1 of 6)](https://medium.com/expedia-group-tech/apache-spark-structured-streaming-first-streaming-example-1-of-6-e8f3219748ef)

2. [Apache Spark Structured Streaming - Input Sources (2 of 6)](https://medium.com/expedia-group-tech/apache-spark-structured-streaming-input-sources-2-of-6-6a72f798838c)

3. [Apache Spark Structured Streaming-Output Sinks (3 of 6)](https://medium.com/expedia-group-tech/apache-spark-structured-streaming-output-sinks-3-of-6-ed3247545fbc)