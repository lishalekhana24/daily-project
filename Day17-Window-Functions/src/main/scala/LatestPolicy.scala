
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object LatestPolicy {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("LatestPolicy")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val policies = Seq(
      ("C101", "P001", "2025-01-10", 5000),
      ("C101", "P002", "2025-05-15", 7000),
      ("C101", "P003", "2026-02-20", 8000),
      ("C102", "P004", "2025-03-12", 4000),
      ("C102", "P005", "2026-01-18", 6000)
    ).toDF("customer", "policy_id", "policy_date", "premium")

    // Group by customer and sort newest first
    val policyWindow = Window
      .partitionBy("customer")
      .orderBy(desc("policy_date"), desc("policy_id"))

    // Assign row number to each policy
    val rankedPolicies = policies
      .withColumn(
        "row_num",
        row_number().over(policyWindow)
      )

    println("All Ranked Policies")
    rankedPolicies.show()

    // Keep only the latest policy for each customer
    val latestPolicies = rankedPolicies
      .filter(col("row_num") === 1)
      .drop("row_num")

    println("Latest Policy Per Customer")
    latestPolicies.show()

    spark.stop()
  }
}

