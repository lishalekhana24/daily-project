import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamsBasics {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------
    // Spark Configuration
    // ------------------------------------------------

    val conf = new SparkConf()
      .setAppName("DStreams Log Error Counter")
      .setMaster("local[2]")

    // ------------------------------------------------
    // Create Streaming Context
    // ------------------------------------------------

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.sparkContext.setLogLevel("ERROR")

    println("\n======================================")
    println("       DAY 23 - DSTREAMS BASICS")
    println("======================================")

    println("\nBatch Interval: 5 seconds")
    println("Waiting for log messages...\n")


    // ------------------------------------------------
    // 1. Read data from socket
    // ------------------------------------------------

    val lines =
      streamingContext.socketTextStream(
        "localhost",
        9999
      )


    // ------------------------------------------------
    // 2. flatMap()
    // ------------------------------------------------

    val words = lines.flatMap(
      line => line.split("\\s+")
    )


    // ------------------------------------------------
    // 3. filter()
    // ------------------------------------------------

    val errorWords = words.filter(
      word => word.toUpperCase == "ERROR"
    )


    // ------------------------------------------------
    // 4. map()
    // ------------------------------------------------

    val errorCount = errorWords.map(
      word => ("ERROR", 1)
    )


    // ------------------------------------------------
    // 5. Count ERROR messages
    // ------------------------------------------------

    val totalErrors = errorCount.reduceByKey(
      (a, b) => a + b
    )


    // ------------------------------------------------
    // 6. Display result every batch
    // ------------------------------------------------

    totalErrors.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n--------------------------------------")
        println("New 5-second batch")
        println("--------------------------------------")

        rdd.collect().foreach {
          case (word, count) =>
            println(word + " messages: " + count)
        }
      }
    }


    // ------------------------------------------------
    // Start Streaming
    // ------------------------------------------------

    streamingContext.start()

    // Keep application running
    streamingContext.awaitTermination()
  }
}
