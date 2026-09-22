import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object Day12CachePersist {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day12 Cache and Persist")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("======================================")
    println("DAY 12 - CACHE AND PERSIST")
    println("======================================")

    // ------------------------------------------------
    // STEP 1: Read transaction data
    // ------------------------------------------------

    val rawData = sc.textFile("data/transactions.csv")

    val header = rawData.first()

    val transactions = rawData
      .filter(line => line != header)
      .map(_.split(","))

    println("\nTotal raw transaction records: " + transactions.count())

    // ------------------------------------------------
    // STEP 2: Clean transaction data
    // ------------------------------------------------

    val cleanedTransactions = transactions
      .filter(fields => fields.length == 6)
      .filter(fields => fields(5) == "COMPLETED")
      .map(fields => (
        fields(0),
        fields(1),
        fields(2),
        fields(3).toDouble,
        fields(4),
        fields(5)
      ))

    // ------------------------------------------------
    // STEP 3: Cache the cleaned RDD
    // ------------------------------------------------

    cleanedTransactions.cache()

    println("\nCleaned transaction count: " +
      cleanedTransactions.count())

    println("Cleaned transaction RDD has been cached.")

    // ------------------------------------------------
    // REPORT 1:
    // Total transaction amount by category
    // ------------------------------------------------

    println("\n========== REPORT 1 ==========")
    println("Total Amount by Category")

    val categoryTotals = cleanedTransactions
      .map(transaction => (transaction._3, transaction._4))
      .reduceByKey(_ + _)

    categoryTotals.collect()
      .sortBy(_._1)
      .foreach {
        case (category, total) =>
          println(category + " -> Rs." + total)
      }

    // ------------------------------------------------
    // REPORT 2:
    // Transaction count by payment method
    // ------------------------------------------------

    println("\n========== REPORT 2 ==========")
    println("Transactions by Payment Method")

    val paymentCounts = cleanedTransactions
      .map(transaction => (transaction._5, 1))
      .reduceByKey(_ + _)

    paymentCounts.collect()
      .sortBy(_._1)
      .foreach {
        case (method, count) =>
          println(method + " -> " + count + " transactions")
      }

    // ------------------------------------------------
    // REPORT 3:
    // High-value transactions
    // ------------------------------------------------

    println("\n========== REPORT 3 ==========")
    println("High Value Transactions (> Rs.20,000)")

    val highValueTransactions = cleanedTransactions
      .filter(transaction => transaction._4 > 20000)

    highValueTransactions.collect()
      .foreach {
        transaction =>
          println(
            transaction._1 +
              " -> " +
              transaction._3 +
              " -> Rs." +
              transaction._4
          )
      }

    // ------------------------------------------------
    // STEP 4: Persist with MEMORY_ONLY
    // ------------------------------------------------

    println("\n========== PERSIST TEST ==========")

    cleanedTransactions.unpersist()

    cleanedTransactions.persist(StorageLevel.MEMORY_ONLY)

    println("Storage level: " +
      cleanedTransactions.getStorageLevel)

    println("Persisted record count: " +
      cleanedTransactions.count())

    // ------------------------------------------------
    // STEP 5: Change storage level
    // ------------------------------------------------

    cleanedTransactions.unpersist()

    cleanedTransactions.persist(StorageLevel.MEMORY_AND_DISK)

    println("\nChanged storage level to:")
    println(cleanedTransactions.getStorageLevel)

    println("Persisted record count: " +
      cleanedTransactions.count())

    // ------------------------------------------------
    // STEP 6: Remove RDD from cache
    // ------------------------------------------------

    cleanedTransactions.unpersist()

    println("\nRDD has been removed from cache.")

    println("\n======================================")
    println("DAY 12 COMPLETED")
    println("======================================")

    sc.stop()
  }
}
