
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

case class Transaction(
  transactionId: String,
  accountId: String,
  branchId: String,
  amount: Double,
  eventTime: String
)

object BankingMonitoring {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day27-Real-Time-Banking")
      .setMaster("local[2]")
      .set("spark.serializer",
        "org.apache.spark.serializer.JavaSerializer")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.sparkContext.setLogLevel("OFF")
    ssc.checkpoint("./checkpoint")

    println("========================================")
    println(" REAL-TIME BANKING MONITORING")
    println("========================================")

    // 1. Small branch/risk reference data
    val branchRiskData = Map(
      "B01" -> "LOW",
      "B02" -> "MEDIUM",
      "B03" -> "HIGH"
    )

    val branchRisk = ssc.sparkContext.broadcast(branchRiskData)

    // 2. Simulated incoming transaction batches
    val batches = mutable.Queue(
      ssc.sparkContext.parallelize(Seq(
        Transaction("T001", "A101", "B01", 500.0, "10:00:01"),
        Transaction("T002", "A101", "B01", 700.0, "10:00:02"),
        Transaction("T003", "A202", "B02", 1500.0, "10:00:03")
      )),
      ssc.sparkContext.parallelize(Seq(
        Transaction("T004", "A101", "B03", 12000.0, "10:00:04"),
        Transaction("T005", "A101", "B03", 15000.0, "10:00:05"),
        Transaction("T006", "A101", "B03", 18000.0, "10:00:06")
      )),
      ssc.sparkContext.parallelize(Seq(
        Transaction("T007", "A202", "B02", 800.0, "10:00:07"),
        Transaction("T008", "A303", "B01", 300.0, "10:00:08")
      )),
      ssc.sparkContext.parallelize(Seq(
        Transaction("T009", "A101", "B03", 22000.0, "10:00:09"),
        Transaction("T010", "A101", "B03", 25000.0, "10:00:10")
      )),
      ssc.sparkContext.emptyRDD[Transaction]
    )

    val transactions =
      ssc.queueStream(batches, oneAtATime = true)

    // Cache and persist transaction data for reuse
    val cachedTransactions =
      transactions.persist(StorageLevel.MEMORY_ONLY)

    // 3. Print each transaction and join with branch risk data
    cachedTransactions.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== TRANSACTIONS ==========")

        rdd.collect().foreach { tx =>
          val risk =
            branchRisk.value.getOrElse(tx.branchId, "UNKNOWN")

          println(
            s"[TRANSACTION] ID=${tx.transactionId}, " +
            s"Account=${tx.accountId}, Branch=${tx.branchId}, " +
            f"Amount=Rs.${tx.amount}%.2f, Risk=$risk"
          )

          if (tx.amount >= 10000 || risk == "HIGH") {
            println(
              s"[REVIEW] Transaction ${tx.transactionId} " +
              s"requires review"
            )
          }
        }
      }
    }

    // 4. Aggregate transaction count and amount per account
    val accountTotals = cachedTransactions
      .map(tx => (tx.accountId, (1, tx.amount)))
      .reduceByKey((a, b) =>
        (a._1 + b._1, a._2 + b._2)
      )

    accountTotals.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== ACCOUNT TOTALS ==========")

        rdd.collect().foreach {
          case (account, (count, total)) =>
            println(
              f"[ACCOUNT] $account%s | Transactions=$count%d | " +
              f"Total=Rs.$total%.2f"
            )
        }
      }
    }

    // 5. Detect bursts: 10-second window, sliding every 4 seconds
    val burstCounts = cachedTransactions
      .map(tx => (tx.accountId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        (a: Int, b: Int) => a - b,
        Seconds(10),
        Seconds(4)
      )

    burstCounts.foreachRDD { rdd =>
      rdd.filter(_._2 >= 3).collect().foreach {
        case (account, count) =>
          println(
            s"[BURST ALERT] Account=$account, " +
            s"Transactions in window=$count"
          )
      }
    }

    // 6. Demonstrate hash partitioning on a batch RDD
    println("\n========== PARTITIONING ==========")

    val partitionedRDD = ssc.sparkContext
      .parallelize(Seq(
        ("A101", 500.0),
        ("A202", 1500.0),
        ("A101", 12000.0),
        ("A303", 300.0)
      ), 2)
      .partitionBy(new HashPartitioner(2))
      .persist(StorageLevel.MEMORY_ONLY)

    partitionedRDD.foreachPartition { records =>
      records.foreach {
        case (account, amount) =>
          println(
            s"[PARTITION] Account=$account, Amount=$amount"
          )
      }
    }

    println("\nStarting stream: batch interval = 2 seconds")
    println("Burst window = 10 seconds; slide = 4 seconds")
    Console.flush()

    ssc.start()
    ssc.awaitTerminationOrTimeout(30000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)

    println("\nBanking Monitoring completed.")
  }
}

