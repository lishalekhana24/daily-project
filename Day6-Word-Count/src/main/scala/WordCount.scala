import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day6-WordCount")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val lines = sc.textFile("data/application.log")

    val wordCounts = lines
      .flatMap { line =>
        line
          .toLowerCase
          .replaceAll("[^a-z0-9]+", " ")
          .split("\\s+")
          .filter(_.nonEmpty)
      }
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("=== Classic Word Count ===")

    wordCounts
      .sortByKey()
      .collect()
      .foreach(println)

    println("\n=== Top 10 Most Frequent Words ===")

    wordCounts
      .sortBy { case (_, count) => -count }
      .take(10)
      .foreach(println)

    spark.stop()
  }
}
