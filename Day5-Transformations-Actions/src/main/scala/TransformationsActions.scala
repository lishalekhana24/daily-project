import org.apache.spark.sql.SparkSession

object TransformationsActions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day5 Transformations and Actions")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println("======================================")
    println("DAY 5 - TRANSFORMATIONS AND ACTIONS")
    println("======================================")

    // ----------------------------------
    // 1. Create RDD
    // ----------------------------------

    val numbers = sc.parallelize(List(1, 2, 3, 4, 5))

    println("\nOriginal RDD:")
    println(numbers.collect().mkString(", "))


    // ----------------------------------
    // 2. MAP
    // ----------------------------------

    val doubled = numbers.map(x => x * 2)

    println("\nMAP - Doubled values:")
    println(doubled.collect().mkString(", "))


    // ----------------------------------
    // 3. FILTER
    // ----------------------------------

    val evenNumbers = numbers.filter(x => x % 2 == 0)

    println("\nFILTER - Even numbers:")
    println(evenNumbers.collect().mkString(", "))


    // ----------------------------------
    // 4. FLATMAP
    // ----------------------------------

    val sentences = sc.parallelize(
      List(
        "Spark is fast",
        "Spark is powerful",
        "Big data with Spark"
      )
    )

    val words = sentences.flatMap(line => line.split(" "))

    println("\nFLATMAP - Words:")
    println(words.collect().mkString(", "))


    // ----------------------------------
    // 5. DISTINCT
    // ----------------------------------

    val duplicateNumbers =
      sc.parallelize(List(1, 2, 2, 3, 3, 3, 4, 4, 5))

    val uniqueNumbers = duplicateNumbers.distinct()

    println("\nDISTINCT - Unique numbers:")
    println(uniqueNumbers.collect().sorted.mkString(", "))


    // ----------------------------------
    // 6. UNION
    // ----------------------------------

    val rdd1 = sc.parallelize(List(1, 2, 3))
    val rdd2 = sc.parallelize(List(4, 5, 6))

    val combined = rdd1.union(rdd2)

    println("\nUNION - Combined RDD:")
    println(combined.collect().mkString(", "))


    // ----------------------------------
    // 7. COUNT
    // ----------------------------------

    val count = numbers.count()

    println("\nCOUNT:")
    println(s"Number of elements = $count")


    // ----------------------------------
    // 8. COLLECT
    // ----------------------------------

    println("\nCOLLECT:")
    println(numbers.collect().mkString(", "))


    // ----------------------------------
    // 9. FIRST
    // ----------------------------------

    println("\nFIRST:")
    println(numbers.first())


    // ----------------------------------
    // 10. TAKE
    // ----------------------------------

    println("\nTAKE:")
    println(numbers.take(3).mkString(", "))


    // ----------------------------------
    // 11. REDUCE
    // ----------------------------------

    val sum = numbers.reduce((a, b) => a + b)

    println("\nREDUCE:")
    println(s"Sum = $sum")


    // ----------------------------------
    // 12. LOG ANALYZER
    // ----------------------------------

    println("\n======================================")
    println("LOG ANALYZER")
    println("======================================")

    val logRDD = sc.textFile("data/app.log")

    println("\nTotal log lines:")
    println(logRDD.count())


    // Filter ERROR messages
    val errorLogs = logRDD.filter(line => line.contains("ERROR"))

    println("\nERROR messages:")
    errorLogs.collect().foreach(println)


    // Count ERROR messages
    val errorCount = errorLogs.count()

    println(s"\nTotal ERROR messages = $errorCount")


    // Extract ERROR text using map
    val errorMessages = errorLogs.map(line => line.split("ERROR")(1).trim)

    println("\nExtracted ERROR messages:")
    errorMessages.collect().foreach(println)


    // ----------------------------------
    // LOG ANALYZER USING FLATMAP
    // ----------------------------------

    val errorWords = errorLogs.flatMap(line => line.split(" "))

    println("\nWords from ERROR logs:")
    println(errorWords.collect().mkString(", "))


    // ----------------------------------
    // DISTINCT ERROR MESSAGES
    // ----------------------------------

    val uniqueErrors = errorMessages.distinct()

    println("\nUnique ERROR messages:")
    uniqueErrors.collect().foreach(println)


    // ----------------------------------
    // FIRST ERROR
    // ----------------------------------

    println("\nFirst ERROR:")
    println(errorLogs.first())


    // ----------------------------------
    // FIRST 2 ERRORS
    // ----------------------------------

    println("\nFirst 2 ERROR messages:")
    errorLogs.take(2).foreach(println)


    spark.stop()
  }
}
