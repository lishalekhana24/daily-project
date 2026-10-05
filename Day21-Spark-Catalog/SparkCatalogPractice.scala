import org.apache.spark.sql.SparkSession

object SparkCatalogPractice {

  def main(args: Array[String]): Unit = {

    // Hide Spark console progress
    System.setProperty("spark.ui.showConsoleProgress", "false")

    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("Spark Catalog Practice")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    // Show only errors
    spark.sparkContext.setLogLevel("ERROR")

    // Required for toDF()
    import spark.implicits._

    println("\n======================================")
    println("       DAY 21 - SPARK CATALOG")
    println("======================================")


    // ------------------------------------------------
    // 1. List Databases
    // ------------------------------------------------

    println("\n1. EXISTING DATABASES:")

    spark.catalog
      .listDatabases()
      .show(false)


    // ------------------------------------------------
    // 2. Create Database
    // ------------------------------------------------

    println("\n2. CREATING HOTEL_ANALYTICS DATABASE:")

    spark.sql("""
      CREATE DATABASE IF NOT EXISTS hotel_analytics
    """)

    println("Database created successfully.")


    // ------------------------------------------------
    // 3. Use Database
    // ------------------------------------------------

    println("\n3. USING HOTEL_ANALYTICS DATABASE:")

    spark.sql("""
      USE hotel_analytics
    """)

    println(
      "Current Database: " + spark.catalog.currentDatabase
    )


    // ------------------------------------------------
    // 4. Create Hotel Booking Data
    // ------------------------------------------------

    println("\n4. HOTEL BOOKING DATA:")

    val bookings = Seq(
      (1, "Lisha", "Taj Hotel", "Delhi", 3, 15000),
      (2, "Rahul", "ITC Hotel", "Mumbai", 2, 10000),
      (3, "Anu", "Taj Hotel", "Delhi", 4, 20000),
      (4, "Priya", "Oberoi", "Chennai", 2, 12000),
      (5, "Arun", "ITC Hotel", "Mumbai", 5, 25000)
    )

    val bookingDF = bookings.toDF(
      "booking_id",
      "guest_name",
      "hotel_name",
      "city",
      "nights",
      "amount"
    )

    bookingDF.show(false)


    // ------------------------------------------------
    // 5. Inspect DataFrame Schema
    // ------------------------------------------------

    println("\n5. BOOKING SCHEMA:")

    bookingDF.printSchema()


    // ------------------------------------------------
    // 6. Create Temporary View
    // ------------------------------------------------

    println("\n6. CREATING TEMPORARY VIEW:")

    bookingDF.createOrReplaceTempView("booking_view")

    println("Temporary view created: booking_view")


    // ------------------------------------------------
    // 7. Query Temporary View
    // ------------------------------------------------

    println("\n7. QUERY TEMPORARY VIEW:")

    spark.sql("""
      SELECT *
      FROM booking_view
    """).show(false)


    // ------------------------------------------------
    // 8. Hotel-wise Revenue
    // ------------------------------------------------

    println("\n8. HOTEL-WISE REVENUE:")

    spark.sql("""
      SELECT
        hotel_name,
        SUM(amount) AS total_revenue,
        COUNT(*) AS total_bookings
      FROM booking_view
      GROUP BY hotel_name
      ORDER BY total_revenue DESC
    """).show(false)


    // ------------------------------------------------
    // 9. City-wise Revenue
    // ------------------------------------------------

    println("\n9. CITY-WISE REVENUE:")

    spark.sql("""
      SELECT
        city,
        SUM(amount) AS total_revenue,
        COUNT(*) AS total_bookings
      FROM booking_view
      GROUP BY city
      ORDER BY total_revenue DESC
    """).show(false)


    // ------------------------------------------------
    // 10. Create Registered Table
    // ------------------------------------------------

    println("\n10. CREATING BOOKINGS TABLE:")

    // Remove previous table registration if it exists
    spark.sql("DROP TABLE IF EXISTS bookings")

    // Remove possible old table location
    val warehousePath =
      "spark-warehouse/hotel_analytics.db/bookings"

    val oldLocation = new java.io.File(warehousePath)

    if (oldLocation.exists()) {
      deleteDirectory(oldLocation)
    }

    // Create table
    bookingDF.write
      .mode("overwrite")
      .saveAsTable("bookings")

    println("Table created successfully: bookings")


    // ------------------------------------------------
    // 11. List Tables
    // ------------------------------------------------

    println("\n11. TABLES IN HOTEL_ANALYTICS:")

    spark.catalog
      .listTables()
      .show(false)


    // ------------------------------------------------
    // 12. Query Registered Table
    // ------------------------------------------------

    println("\n12. QUERY BOOKINGS TABLE:")

    spark.sql("""
      SELECT *
      FROM bookings
    """).show(false)


    // ------------------------------------------------
    // 13. Inspect Table Metadata
    // ------------------------------------------------

    println("\n13. BOOKINGS TABLE METADATA:")

    val table = spark.catalog.getTable("bookings")

    println("Table Name   : " + table.name)
    println("Database     : " + table.database)
    println("Table Type   : " + table.tableType)
    println("Temporary    : " + table.isTemporary)


    // ------------------------------------------------
    // 14. Inspect Columns
    // ------------------------------------------------

    println("\n14. BOOKINGS TABLE COLUMNS:")

    spark.catalog
      .listColumns("bookings")
      .show(false)


    // ------------------------------------------------
    // 15. Describe Table
    // ------------------------------------------------

    println("\n15. DESCRIBE BOOKINGS TABLE:")

    spark.sql("""
      DESCRIBE TABLE bookings
    """).show(false)


    // ------------------------------------------------
    // 16. Query Using Condition
    // ------------------------------------------------

    println("\n16. BOOKINGS WITH AMOUNT > 15000:")

    spark.sql("""
      SELECT
        booking_id,
        guest_name,
        hotel_name,
        city,
        nights,
        amount
      FROM bookings
      WHERE amount > 15000
      ORDER BY amount DESC
    """).show(false)


    // ------------------------------------------------
    // 17. Final Database List
    // ------------------------------------------------

    println("\n17. FINAL DATABASE LIST:")

    spark.catalog
      .listDatabases()
      .show(false)


    println("\n======================================")
    println("       DAY 21 COMPLETED SUCCESSFULLY")
    println("======================================")


    // Stop Spark
    spark.stop()
  }


  // ------------------------------------------------
  // Helper method to delete old table directory
  // ------------------------------------------------

  def deleteDirectory(file: java.io.File): Unit = {

    if (file.isDirectory) {
      file.listFiles().foreach(deleteDirectory)
    }

    file.delete()
  }
}
