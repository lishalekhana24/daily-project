import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLBasics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day13-Spark-SQL-Basics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Step 1: Read CSV file
    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("===== CUSTOMER DATA =====")
    customers.show()

    // Step 2: Inspect schema
    println("===== SCHEMA =====")
    customers.printSchema()

    // Step 3: Select columns
    println("===== SELECT CUSTOMER NAME AND CITY =====")
    customers
      .select("name", "city")
      .show()

    // Step 4: Filter customers
    println("===== CUSTOMERS WITH SPENDING > 7000 =====")
    customers
      .filter(col("spending") > 7000)
      .show()

    // Step 5: Create a new column
    val customerAnalytics = customers.withColumn(
      "customer_category",
      when(col("spending") >= 10000, "Premium")
        .when(col("spending") >= 5000, "Regular")
        .otherwise("Basic")
    )

    println("===== CUSTOMER CATEGORY =====")
    customerAnalytics.show()

    // Step 6: Register temporary view
    customerAnalytics.createOrReplaceTempView("customers")

    // Step 7: Run SQL query
    println("===== PREMIUM CUSTOMERS =====")

    val premiumCustomers = spark.sql(
      """
        SELECT customer_id, name, city, spending, customer_category
        FROM customers
        WHERE customer_category = 'Premium'
      """
    )

    premiumCustomers.show()

    // Step 8: City-wise analytics
    println("===== CITY-WISE CUSTOMER ANALYTICS =====")

    val cityAnalytics = spark.sql(
      """
        SELECT
          city,
          COUNT(*) AS customer_count,
          ROUND(AVG(spending), 2) AS average_spending,
          SUM(spending) AS total_spending
        FROM customers
        GROUP BY city
        ORDER BY total_spending DESC
      """
    )

    cityAnalytics.show()

    // Step 9: Stop Spark
    spark.stop()
  }
}
