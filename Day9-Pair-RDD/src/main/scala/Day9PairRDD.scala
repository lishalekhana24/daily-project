import org.apache.spark.{SparkConf, SparkContext}

object Day9PairRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day9-Pair-RDD")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // --------------------------------------------------
    // 1. Create Key-Value Pair RDD
    // --------------------------------------------------

    val sales = Seq(
      ("Laptop", "Electronics", 2, 50000),
      ("Mouse", "Electronics", 5, 500),
      ("Keyboard", "Electronics", 3, 1500),
      ("Laptop", "Electronics", 1, 50000),
      ("Chair", "Furniture", 4, 3000),
      ("Table", "Furniture", 2, 7000),
      ("Chair", "Furniture", 2, 3000),
      ("Phone", "Electronics", 3, 25000)
    )

    val salesRDD = sc.parallelize(sales)

    println("\n========== ORIGINAL SALES DATA ==========")
    salesRDD.collect().foreach(println)

    // --------------------------------------------------
    // 2. Revenue by Product using reduceByKey
    // --------------------------------------------------

    val productRevenue = salesRDD
      .map {
        case (product, department, quantity, price) =>
          (product, quantity * price)
      }
      .reduceByKey(_ + _)

    println("\n========== REVENUE BY PRODUCT ==========")
    productRevenue.collect().sortBy(_._1).foreach(println)

    // --------------------------------------------------
    // 3. Revenue by Department using reduceByKey
    // --------------------------------------------------

    val departmentRevenue = salesRDD
      .map {
        case (product, department, quantity, price) =>
          (department, quantity * price)
      }
      .reduceByKey(_ + _)

    println("\n========== REVENUE BY DEPARTMENT ==========")
    departmentRevenue.collect().sortBy(_._1).foreach(println)

    // --------------------------------------------------
    // 4. mapValues Example
    // --------------------------------------------------

    val productQuantities = salesRDD
      .map {
        case (product, department, quantity, price) =>
          (product, quantity)
      }

    val doubledQuantities = productQuantities.mapValues(q => q * 2)

    println("\n========== MAPVALUES - DOUBLED QUANTITIES ==========")
    doubledQuantities.collect().foreach(println)

    // --------------------------------------------------
    // 5. groupByKey Example
    // --------------------------------------------------

    val groupedProducts = productQuantities.groupByKey()

    println("\n========== GROUPBYKEY - QUANTITIES BY PRODUCT ==========")

    groupedProducts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (product, quantities) =>
          println(product + " -> " + quantities.mkString(", "))
      }

    // --------------------------------------------------
    // 6. Compare reduceByKey and groupByKey
    // --------------------------------------------------

    val startReduce = System.nanoTime()

    val reduceResult = productQuantities
      .reduceByKey(_ + _)
      .collect()

    val endReduce = System.nanoTime()

    val reduceTime = (endReduce - startReduce) / 1e6

    val startGroup = System.nanoTime()

    val groupResult = productQuantities
      .groupByKey()
      .mapValues(_.sum)
      .collect()

    val endGroup = System.nanoTime()

    val groupTime = (endGroup - startGroup) / 1e6

    println("\n========== PERFORMANCE COMPARISON ==========")

    println("reduceByKey result:")
    reduceResult.sortBy(_._1).foreach(println)

    println(f"reduceByKey execution time: $reduceTime%.3f ms")

    println("\ngroupByKey result:")
    groupResult.sortBy(_._1).foreach(println)

    println(f"groupByKey + mapValues execution time: $groupTime%.3f ms")

    // --------------------------------------------------
    // 7. Bank Transaction Scenario
    // --------------------------------------------------

    val transactions = Seq(
      ("ACC101", 5000.0),
      ("ACC102", 3000.0),
      ("ACC101", 2500.0),
      ("ACC103", 7000.0),
      ("ACC102", 1500.0),
      ("ACC101", 1000.0),
      ("ACC103", 2000.0),
      ("ACC104", 4500.0)
    )

    val transactionRDD = sc.parallelize(transactions)

    val accountTotals = transactionRDD
      .reduceByKey(_ + _)

    println("\n========== BANK TRANSACTIONS BY ACCOUNT ==========")

    accountTotals
      .collect()
      .sortBy(_._1)
      .foreach {
        case (account, amount) =>
          println(f"$account -> Rs. $amount%.2f")
      }

    // --------------------------------------------------
    // 8. Number of Partitions
    // --------------------------------------------------

    println("\n========== PARTITION INFORMATION ==========")
    println("Sales RDD partitions: " + salesRDD.getNumPartitions)
    println("Transaction RDD partitions: " + transactionRDD.getNumPartitions)

    sc.stop()
  }
}
