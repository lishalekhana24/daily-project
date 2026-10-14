
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds

object StatelessVsStatefulStreaming {

  def main(args: Array[String]): Unit = {
// Hide Spark INFO and WARNING logs
    org.apache.logging.log4j.core.config.Configurator.setRootLevel(
      org.apache.logging.log4j.Level.ERROR
    )
    val conf = new SparkConf()
      .setAppName("StatelessVsStatefulStreaming")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Required for stateful processing
    ssc.checkpoint("checkpoint-day24")

    // Receive transactions through a TCP socket
    val lines = ssc.socketTextStream("localhost", 9999)

    // Input format: accountId,amount
    val transactions = lines.map { line =>
      val parts = line.split(",")
      (parts(0).trim, parts(1).trim.toDouble)
    }

    // 1. Stateless: count transactions in each batch only
    val currentBatchCounts = transactions
      .map { case (account, amount) => (account, 1) }
      .reduceByKey(_ + _)

    currentBatchCounts.foreachRDD { rdd =>
      println("\n--- CURRENT BATCH COUNTS ---")
      rdd.collect().foreach(println)
    }

    // 2. Stateful: maintain running transaction counts
    val accountCounts = transactions
      .map { case (account, amount) => (account, 1) }

    val runningCounts = accountCounts.updateStateByKey[Int] {
      (newCounts: Seq[Int], previousState: Option[Int]) =>
        Some(newCounts.sum + previousState.getOrElse(0))
    }

    runningCounts.foreachRDD { rdd =>
      println("\n--- RUNNING TRANSACTION COUNTS ---")
      rdd.collect().foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

