
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object LagLeadPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("LagLeadPractice")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val exams = Seq(
      ("Lisha", 1, 70),
      ("Lisha", 2, 80),
      ("Lisha", 3, 90),
      ("Priya", 1, 85),
      ("Priya", 2, 88),
      ("Priya", 3, 95)
    ).toDF("student", "exam", "marks")

    val examWindow = Window
      .partitionBy("student")
      .orderBy("exam")

    val result = exams
      .withColumn(
        "previous_marks",
        lag("marks", 1).over(examWindow)
      )
      .withColumn(
        "next_marks",
        lead("marks", 1).over(examWindow)
      )

    result.show()

    spark.stop()
  }
}

