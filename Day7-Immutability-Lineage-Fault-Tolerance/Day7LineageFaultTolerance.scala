import org.apache.spark.{SparkConf, SparkContext}

object Day7LineageFaultTolerance {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day7-Immutability-Lineage-Fault-Tolerance")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("======================================")
    println("DAY 7 - RDD IMMUTABILITY, LINEAGE")
    println("AND FAULT TOLERANCE")
    println("======================================")

    // --------------------------------------------------
    // STEP 1: Create the original RDD
    // --------------------------------------------------

    val numbers = sc.parallelize(1 to 20, 4)

    println("\nOriginal RDD:")
    println(numbers.collect().mkString(", "))


    // --------------------------------------------------
    // STEP 2: Filter even numbers
    // --------------------------------------------------

    val evenNumbers = numbers.filter(_ % 2 == 0)

    println("\nEven Numbers:")
    println(evenNumbers.collect().mkString(", "))


    // --------------------------------------------------
    // STEP 3: Multiply each number by 10
    // --------------------------------------------------

    val multipliedNumbers = evenNumbers.map(_ * 10)

    println("\nMultiplied Numbers:")
    println(multipliedNumbers.collect().mkString(", "))


    // --------------------------------------------------
    // STEP 4: Keep numbers greater than 50
    // --------------------------------------------------

    val filteredNumbers = multipliedNumbers.filter(_ > 50)

    println("\nNumbers greater than 50:")
    println(filteredNumbers.collect().mkString(", "))


    // --------------------------------------------------
    // STEP 5: Create final transformation
    // --------------------------------------------------

    val finalResult = filteredNumbers.map(_ + 5)

    println("\nFinal Result:")
    println(finalResult.collect().mkString(", "))


    // --------------------------------------------------
    // STEP 6: Display RDD Lineage
    // --------------------------------------------------

    println("\n======================================")
    println("RDD LINEAGE")
    println("======================================")

    println(finalResult.toDebugString)


    // --------------------------------------------------
    // STEP 7: Demonstrate Immutability
    // --------------------------------------------------

    println("\n======================================")
    println("RDD IMMUTABILITY")
    println("======================================")

    val original = sc.parallelize(List(10, 20, 30, 40))

    val transformed = original.map(_ * 2)

    println("Original RDD:")
    println(original.collect().mkString(", "))

    println("Transformed RDD:")
    println(transformed.collect().mkString(", "))

    println("The original RDD remains unchanged.")


    // --------------------------------------------------
    // STEP 8: Simulate executor loss conceptually
    // --------------------------------------------------

    println("\n======================================")
    println("FAULT TOLERANCE SIMULATION")
    println("======================================")

    println("Assume one executor loses a partition.")
    println("Spark checks the RDD lineage.")
    println("The lost partition is recomputed from the previous transformations.")
    println("The complete RDD does not need to be recreated.")
    println("This demonstrates Spark's fault-tolerance mechanism.")


    // --------------------------------------------------
    // STEP 9: Stop Spark
    // --------------------------------------------------

    sc.stop()
  }
}
