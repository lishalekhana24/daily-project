import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

object Day10Partitioning {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day10-Partitioning")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("==========================================")
    println("DAY 10 - PARTITIONING")
    println("==========================================")

    // --------------------------------------------------
    // 1. Create an RDD with too few partitions
    // --------------------------------------------------

    val numbers = 1 to 20

    val rdd = sc.parallelize(numbers, 2)

    println("\n1. Initial RDD")
    println("Number of elements: " + rdd.count())
    println("Initial partitions: " + rdd.getNumPartitions)

    // --------------------------------------------------
    // 2. Inspect partition contents
    // --------------------------------------------------

    println("\n2. Initial Partition Contents")

    rdd.mapPartitionsWithIndex {
      case (index, iterator) =>
        Iterator(
          s"Partition $index: ${iterator.mkString(", ")}"
        )
    }.collect().foreach(println)

    // --------------------------------------------------
    // 3. Repartition - increase partitions
    // --------------------------------------------------

    val repartitionedRDD = rdd.repartition(4)

    println("\n3. After repartition(4)")
    println("Number of partitions: " +
      repartitionedRDD.getNumPartitions)

    repartitionedRDD.mapPartitionsWithIndex {
      case (index, iterator) =>
        Iterator(
          s"Partition $index: ${iterator.mkString(", ")}"
        )
    }.collect().foreach(println)

    // --------------------------------------------------
    // 4. Coalesce - decrease partitions
    // --------------------------------------------------

    val coalescedRDD = repartitionedRDD.coalesce(2)

    println("\n4. After coalesce(2)")
    println("Number of partitions: " +
      coalescedRDD.getNumPartitions)

    coalescedRDD.mapPartitionsWithIndex {
      case (index, iterator) =>
        Iterator(
          s"Partition $index: ${iterator.mkString(", ")}"
        )
    }.collect().foreach(println)

    // --------------------------------------------------
    // 5. Pair RDD
    // --------------------------------------------------

    val sales = sc.parallelize(
      Seq(
        ("Laptop", 100000),
        ("Mouse", 500),
        ("Keyboard", 1500),
        ("Laptop", 80000),
        ("Mouse", 700),
        ("Monitor", 20000),
        ("Keyboard", 2000),
        ("Laptop", 90000)
      ),
      2
    )

    println("\n5. Pair RDD")
    println("Initial partitions: " +
      sales.getNumPartitions)

    // --------------------------------------------------
    // 6. partitionBy
    // --------------------------------------------------

    val partitionedSales =
      sales.partitionBy(new HashPartitioner(4))

    println("\n6. After partitionBy(HashPartitioner(4))")
    println("Number of partitions: " +
      partitionedSales.getNumPartitions)

    partitionedSales.mapPartitionsWithIndex {
      case (index, iterator) =>
        Iterator(
          s"Partition $index: ${iterator.mkString(", ")}"
        )
    }.collect().foreach(println)

    // --------------------------------------------------
    // 7. Aggregate data
    // --------------------------------------------------

    val totalSales = partitionedSales
      .reduceByKey(_ + _)

    println("\n7. Total Sales By Product")

    totalSales.collect().foreach {
      case (product, amount) =>
        println(s"$product -> $amount")
    }

    // --------------------------------------------------
    // 8. Scenario: dataset with too few partitions
    // --------------------------------------------------

    println("\n8. Scenario: Too Few Partitions")

    val smallPartitionRDD =
      sc.parallelize(1 to 100, 1)

    println(
      "Original partitions: " +
        smallPartitionRDD.getNumPartitions
    )

    val optimizedRDD =
      smallPartitionRDD.repartition(4)

    println(
      "Optimized partitions: " +
        optimizedRDD.getNumPartitions
    )

    println(
      "Dataset count after repartition: " +
        optimizedRDD.count()
    )

    println("\n==========================================")
    println("DAY 10 COMPLETED")
    println("==========================================")

    sc.stop()
  }
}
