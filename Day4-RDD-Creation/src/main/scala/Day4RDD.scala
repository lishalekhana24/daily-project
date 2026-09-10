import org.apache.spark.{SparkConf, SparkContext}

object Day4RDD {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // Spark configuration
    // --------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day4 RDD Creation")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("========== DAY 4 RDD CREATION ==========")

    // --------------------------------------------------
    // 1. Create RDD from Scala collection
    // --------------------------------------------------

    val numbers = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numberRDD = sc.parallelize(numbers)

    println("\n1. RDD CREATED FROM COLLECTION")
    println("Original data: " + numberRDD.collect().mkString(", "))

    // --------------------------------------------------
    // 2. map operation
    // --------------------------------------------------

    val squaredRDD = numberRDD.map(x => x * x)

    println("\n2. MAP OPERATION")
    println("Squared values: " + squaredRDD.collect().mkString(", "))

    // --------------------------------------------------
    // 3. filter operation
    // --------------------------------------------------

    val evenRDD = numberRDD.filter(x => x % 2 == 0)

    println("\n3. FILTER OPERATION")
    println("Even numbers: " + evenRDD.collect().mkString(", "))

    // --------------------------------------------------
    // 4. flatMap operation
    // --------------------------------------------------

    val sentences = Seq(
      "Spark is fast",
      "RDD is distributed",
      "Scala is powerful"
    )

    val sentenceRDD = sc.parallelize(sentences)

    val wordsRDD = sentenceRDD.flatMap(line => line.split(" "))

    println("\n4. FLATMAP OPERATION")
    println("Words: " + wordsRDD.collect().mkString(", "))

    // --------------------------------------------------
    // 5. Read transactions from text file
    // --------------------------------------------------

    val transactionRDD =
      sc.textFile("data/transactions.txt")

    println("\n5. TRANSACTION RDD")
    println("Number of transactions: " + transactionRDD.count())

    transactionRDD.collect().foreach(println)

    // --------------------------------------------------
    // 6. Calculate total sales
    // --------------------------------------------------

    val totalSales = transactionRDD
      .map(line => line.split(","))
      .map(fields => fields(3).toDouble)
      .sum()

    println("\n6. TOTAL SALES")
    println("Total sales: " + totalSales)

    // --------------------------------------------------
    // 7. Filter high-value transactions
    // --------------------------------------------------

    val highValueTransactions = transactionRDD
      .filter(line => line.split(",")(3).toDouble >= 500)

    println("\n7. HIGH VALUE TRANSACTIONS")
    highValueTransactions.collect().foreach(println)

    // --------------------------------------------------
    // 8. Inspect partitions
    // --------------------------------------------------

    println("\n8. PARTITION INFORMATION")

    println(
      "Number of partitions in numberRDD: " +
      numberRDD.getNumPartitions
    )

    println(
      "Number of partitions in transactionRDD: " +
      transactionRDD.getNumPartitions
    )

    println(
      "Default parallelism: " +
      sc.defaultParallelism
    )

    // --------------------------------------------------
    // 9. Process customer file
    // --------------------------------------------------

    val customerRDD =
      sc.textFile("data/customers.txt", 4)

    println("\n9. CUSTOMER FILE")

    println(
      "Customer RDD partitions: " +
      customerRDD.getNumPartitions
    )

    println("Customer records:")

    customerRDD.collect().foreach(println)
// --------------------------------------------------
// Large customer file scenario
// --------------------------------------------------

val largeCustomerRDD =
  sc.textFile("data/large_customers.txt", 4)

println("\n10. LARGE CUSTOMER FILE")

println(
  "Number of customer records: " +
  largeCustomerRDD.count()
)

println(
  "Number of partitions: " +
  largeCustomerRDD.getNumPartitions
)

val customersFromHyderabad =
  largeCustomerRDD.filter(
    line => line.endsWith("Hyderabad")
  )

println(
  "Hyderabad customers: " +
  customersFromHyderabad.count()
)

    // --------------------------------------------------
    // Stop Spark
    // --------------------------------------------------

    sc.stop()

    println("\n========== APPLICATION COMPLETED ==========")
  }
}


