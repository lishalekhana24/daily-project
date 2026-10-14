import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.concurrent.duration._

object WindowOperations {

  def main(args: Array[String]): Unit = {

    // true = quick demonstration; false = actual 10-minute window
    val demoMode = false

    val batchInterval =
      if (demoMode) Seconds(2) else Seconds(2)

    val windowSize =
      if (demoMode) Seconds(10) else Minutes(10)

    val slidingInterval =
      if (demoMode) Seconds(4) else Minutes(2)

    val conf = new SparkConf()
      .setAppName("Day25-Window-Operations")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, batchInterval)
    ssc.sparkContext.setLogLevel("ERROR")

    // Required for window operations and state recovery
    ssc.checkpoint("./checkpoint")

    // Sample transaction data
    val transactionBatches = Seq(
      Seq("T01", "T02"),
      Seq("T03", "T04"),
      Seq("T05", "T06"),
      Seq("T07", "T08", "T09", "T10"),
      Seq("T11", "T12", "T13", "T14"),
      Seq("T15", "T16", "T17", "T18", "T19"),
      Seq("T20", "T21"),
      Seq("T22", "T23", "T24", "T25"),
      Seq("T26", "T27"),
      Seq("T28", "T29", "T30")
    )

    val transactionQueue =
      mutable.Queue[RDD[String]]()

    transactionBatches.foreach { batch =>
      transactionQueue.enqueue(
        ssc.sparkContext.parallelize(batch)
      )
    }

    val transactions = ssc.queueStream(
      transactionQueue,
      oneAtATime = true
    )

    // Operation 1: Count transactions in the window
    val transactionCount = transactions.countByWindow(
      windowSize,
      slidingInterval
    )

    transactionCount.foreachRDD { rdd =>
      rdd.collect().foreach { count =>
        println(s"[WINDOW COUNT] Transactions in window: $count")

        // Demonstration threshold
        if (count >= 6) {
          println(
            s"[ALERT] Sudden transaction increase detected: $count transactions"
          )
        }
      }
    }

    // Sample sales data: product -> sales amount
    val salesBatches = Seq(
      Seq(("Laptop", 50000.0), ("Phone", 20000.0)),
      Seq(("Laptop", 45000.0), ("Phone", 15000.0)),
      Seq(("Laptop", 55000.0), ("Phone", 18000.0)),
      Seq(("Laptop", 60000.0), ("Phone", 25000.0)),
      Seq(("Laptop", 40000.0), ("Phone", 12000.0)),
      Seq(("Laptop", 70000.0), ("Phone", 30000.0)),
      Seq(("Laptop", 48000.0), ("Phone", 16000.0)),
      Seq(("Laptop", 52000.0), ("Phone", 22000.0)),
      Seq(("Laptop", 65000.0), ("Phone", 27000.0)),
      Seq(("Laptop", 75000.0), ("Phone", 35000.0))
    )

    val salesQueue =
      mutable.Queue[RDD[(String, Double)]]()

    salesBatches.foreach { batch =>
      salesQueue.enqueue(
        ssc.sparkContext.parallelize(batch)
      )
    }

    val sales = ssc.queueStream(
      salesQueue,
      oneAtATime = true
    )

    // Operation 2: Calculate rolling sales for each product
    val rollingSales = sales.reduceByKeyAndWindow(
      (a: Double, b: Double) => a + b,
      windowSize,
      slidingInterval
    )

    rollingSales.foreachRDD { rdd =>
      val results = rdd.collect().sortBy(_._1)

      if (results.nonEmpty) {
        println("[ROLLING SALES] Total sales in current window:")

        results.foreach { case (product, total) =>
          println(f"  $product%s -> Rs. $total%.2f")
        }
      }
    }

    println(s"Batch interval: $batchInterval")
    println(s"Window size: $windowSize")
    println(s"Sliding interval: $slidingInterval")
    println("Starting Window Operations...")

    ssc.start()
    ssc.awaitTerminationOrTimeout(35000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
