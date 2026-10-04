
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionsPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day17-Window-Functions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Student data
    val students = Seq(
      ("Lisha", "Java", 95),
      ("Priya", "Java", 85),
      ("Arun", "Java", 85),
      ("Ravi", "Java", 70),
      ("Sneha", "Python", 98),
      ("Rahul", "Python", 88),
      ("Anu", "Python", 88),
      ("Kiran", "Python", 75)
    ).toDF("student", "course", "marks")

    println("Original Student Data")
    students.show()

    // Create a window partitioned by course
    // and ordered by marks
    val windowSpec = Window
      .partitionBy("course")
      .orderBy(desc("marks"), asc("student"))

    // Apply ranking functions
    val result = students
      .withColumn("row_number", row_number().over(windowSpec))
      .withColumn("rank", rank().over(windowSpec))
      .withColumn("dense_rank", dense_rank().over(windowSpec))

    println("Student Ranking")
    result.show()

    // Top 3 students per course
    val top3 = result.filter(col("row_number") <= 3)

    println("Top 3 Students Per Course")
    top3.show()

    spark.stop()
  }
}

