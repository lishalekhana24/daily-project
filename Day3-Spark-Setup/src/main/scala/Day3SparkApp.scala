import org.apache.spark.sql.SparkSession

object Day3SparkApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day3 Spark Application")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("====================================")
    println("Day 3 - Spark First Application")
    println("====================================")

    println("Application Name: " + spark.sparkContext.appName)
    println("Master: " + spark.sparkContext.master)

    println("\nReading input file...")

    val lines = sc.textFile("data/input.txt")

    println("\nFile Contents:")
    lines.collect().foreach(println)

    println("\nTotal number of lines: " + lines.count())

    spark.stop()
  }
}
