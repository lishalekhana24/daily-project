
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FileFormatsPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20-File-Formats")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample daily sales data
    val sales = Seq(
      (1, "Chennai", "Laptop", 55000.0, "2026-01-10"),
      (2, "Bangalore", "Mobile", 20000.0, "2026-01-10"),
      (3, "Hyderabad", "Tablet", 15000.0, "2026-01-11"),
      (4, "Chennai", "Mobile", 25000.0, "2026-02-05"),
      (5, "Bangalore", "Laptop", 60000.0, "2026-02-05"),
      (6, "Puducherry", "Tablet", 18000.0, "2026-02-06"),
      (7, "Chennai", "Laptop", 58000.0, "2025-12-20"),
      (8, "Hyderabad", "Mobile", 22000.0, "2025-12-21")
    ).toDF(
      "sale_id", "city", "product", "amount", "sale_date"
    )

    println("ORIGINAL SALES DATA")
    sales.show()

    // 1. Write CSV
    sales.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/sales_csv")

    // 2. Write JSON
    sales.write
      .mode("overwrite")
      .json("output/sales_json")

    // 3. Write Parquet
    sales.write
      .mode("overwrite")
      .parquet("output/sales_parquet")

    // Read CSV
    val csvData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("output/sales_csv")

    println("READ CSV")
    csvData.show()

    // Read JSON
    val jsonData = spark.read
      .json("output/sales_json")

    println("READ JSON")
    jsonData.show()

    // Read Parquet
    val parquetData = spark.read
      .parquet("output/sales_parquet")

    println("READ PARQUET")
    parquetData.show()

    // Add year, month and day
    val dailySales = sales
      .withColumn("sale_date", to_date(col("sale_date")))
      .withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .withColumn("day", dayofmonth(col("sale_date")))

    println("SALES WITH DATE PARTITIONS")
    dailySales.show()

    // Repartition before writing
    val repartitionedSales = dailySales
      .repartition(4, col("year"), col("month"), col("day"))

    // Write partitioned Parquet output
    repartitionedSales.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/daily_sales")

    println("PARTITIONED DATA WRITTEN SUCCESSFULLY")

    // Read partitioned data
    val partitionedData = spark.read
      .parquet("output/daily_sales")

    println("READ PARTITIONED SALES")
    partitionedData.show()

    println("TOTAL SALES RECORDS")
    println(partitionedData.count())

    spark.stop()
  }
}

