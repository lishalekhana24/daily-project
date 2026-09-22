import org.apache.spark.{SparkConf, SparkContext}

object Day11BroadcastAccumulators {

  def main(args: Array[String]): Unit = {

    // ----------------------------------------------------
    // Step 1: Create Spark configuration and SparkContext
    // ----------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day11-Broadcast-Accumulators")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // ----------------------------------------------------
    // Step 2: Create a small product master table
    // ----------------------------------------------------

    val productMaster = Map(
      "P101" -> ("Laptop", 55000.0),
      "P102" -> ("Mouse", 800.0),
      "P103" -> ("Keyboard", 1500.0),
      "P104" -> ("Monitor", 12000.0),
      "P105" -> ("Headphones", 2500.0)
    )

    // ----------------------------------------------------
    // Step 3: Broadcast the product master table
    // ----------------------------------------------------

    val broadcastProducts = sc.broadcast(productMaster)

    // ----------------------------------------------------
    // Step 4: Create accumulator for bad records
    // ----------------------------------------------------

    val badRecords = sc.longAccumulator("Bad Records")

    // ----------------------------------------------------
    // Step 5: Read transaction data
    // ----------------------------------------------------

    val transactions = sc.textFile("data/transactions.txt")

    // ----------------------------------------------------
    // Step 6: Validate transactions
    // ----------------------------------------------------

    val validatedTransactions = transactions.map { line =>

      val parts = line.split(",")

      val transactionId = parts(0)
      val productId = parts(1)
      val quantity = parts(2).toInt

      val products = broadcastProducts.value

      if (products.contains(productId)) {

        val product = products(productId)
        val productName = product._1
        val price = product._2

        val totalAmount = quantity * price

        s"$transactionId | VALID | $productId | $productName | Quantity=$quantity | Total=$totalAmount"

      } else {

        badRecords.add(1)

        s"$transactionId | INVALID | Unknown Product=$productId"
      }
    }

    // ----------------------------------------------------
    // Step 7: Trigger Spark execution
    // ----------------------------------------------------

    val results = validatedTransactions.collect()

    // ----------------------------------------------------
    // Step 8: Display results
    // ----------------------------------------------------

    println("\n===== TRANSACTION VALIDATION RESULTS =====")

    results.foreach(println)

    // ----------------------------------------------------
    // Step 9: Display accumulator value
    // ----------------------------------------------------

    println("\n===== DATA QUALITY SUMMARY =====")

    println(s"Total Transactions: ${transactions.count()}")
    println(s"Bad Records: ${badRecords.value}")
    println(s"Valid Records: ${transactions.count() - badRecords.value}")

    // ----------------------------------------------------
    // Step 10: Stop Spark
    // ----------------------------------------------------

    sc.stop()
  }
}
