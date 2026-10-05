import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchSalesPipeline {

  def main(args: Array[String]): Unit = {

    // Hide Spark console progress
    System.setProperty("spark.ui.showConsoleProgress", "false")

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("E-Commerce Daily Sales Pipeline")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println("\n==========================================")
    println("   DAY 22 - BATCH SALES PIPELINE")
    println("==========================================")


    // ------------------------------------------------
    // 1. Read Raw Transaction Data
    // ------------------------------------------------

    println("\n1. READING RAW TRANSACTIONS:")

    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    transactions.show(false)

    println("Raw transaction count: " + transactions.count())


    // ------------------------------------------------
    // 2. Read Customer Data
    // ------------------------------------------------

    println("\n2. READING CUSTOMER DATA:")

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    customers.show(false)


    // ------------------------------------------------
    // 3. Read Product Data
    // ------------------------------------------------

    println("\n3. READING PRODUCT DATA:")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    products.show(false)


    // ------------------------------------------------
    // 4. Clean Invalid Transactions
    // ------------------------------------------------

    println("\n4. CLEANING INVALID TRANSACTIONS:")

    val validTransactions = transactions
      .filter(col("quantity") > 0)
      .filter(col("unit_price") > 0)
      .filter(col("transaction_id").isNotNull)
      .filter(col("customer_id").isNotNull)
      .filter(col("product_id").isNotNull)
      .filter(col("transaction_date").isNotNull)

    println(
      "Transactions after basic validation: "
        + validTransactions.count()
    )

    validTransactions.show(false)


    // ------------------------------------------------
    // 5. Join with Customer Data
    // ------------------------------------------------

    println("\n5. JOINING WITH CUSTOMER DATA:")

    val customerJoined = validTransactions
      .join(
        customers,
        Seq("customer_id"),
        "inner"
      )

    customerJoined.show(false)

    println(
      "Records after customer join: "
        + customerJoined.count()
    )


    // ------------------------------------------------
    // 6. Join with Product Data
    // ------------------------------------------------

    println("\n6. JOINING WITH PRODUCT DATA:")

    val enrichedSales = customerJoined
      .join(
        products,
        Seq("product_id"),
        "inner"
      )

    enrichedSales.show(false)

    println(
      "Records after product join: "
        + enrichedSales.count()
    )


    // ------------------------------------------------
    // 7. Calculate Revenue
    // ------------------------------------------------

    println("\n7. CALCULATING REVENUE:")

    val salesWithRevenue = enrichedSales
      .withColumn(
        "revenue",
        col("quantity") * col("unit_price")
      )

    salesWithRevenue.show(false)


    // ------------------------------------------------
    // 8. Aggregate Revenue
    // ------------------------------------------------

    println("\n8. DAILY SALES AGGREGATION:")

    val dailySales = salesWithRevenue
      .groupBy(
        "transaction_date",
        "city",
        "category"
      )
      .agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        count("transaction_id").alias("total_transactions")
      )
      .orderBy(
        "transaction_date",
        "city",
        "category"
      )

    dailySales.show(false)


    // ------------------------------------------------
    // 9. Write Partitioned Parquet Output
    // ------------------------------------------------

    println("\n9. WRITING PARTITIONED PARQUET:")

    dailySales.write
      .mode("overwrite")
      .partitionBy("transaction_date")
      .parquet("output/daily_sales")

    println("Parquet output written successfully.")


    // ------------------------------------------------
    // 10. Read the Output Back
    // ------------------------------------------------

    println("\n10. READING PARQUET OUTPUT:")

    val finalOutput = spark.read
      .parquet("output/daily_sales")

    finalOutput.show(false)


    // ------------------------------------------------
    // 11. Display Final Schema
    // ------------------------------------------------

    println("\n11. FINAL OUTPUT SCHEMA:")

    finalOutput.printSchema()


    // ------------------------------------------------
    // 12. Final Summary
    // ------------------------------------------------

    println("\n==========================================")
    println("   BATCH PIPELINE COMPLETED SUCCESSFULLY")
    println("==========================================")

    println(
      "Final aggregated records: "
        + finalOutput.count()
    )

    spark.stop()
  }
}
