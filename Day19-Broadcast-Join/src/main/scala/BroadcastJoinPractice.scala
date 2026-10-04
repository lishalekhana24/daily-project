
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast

object BroadcastJoinPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19-Broadcast-Join")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Small reference DataFrame
    val branches = Seq(
      (101, "Main Branch", "Chennai"),
      (102, "City Branch", "Bangalore"),
      (103, "Central Branch", "Hyderabad"),
      (104, "North Branch", "Puducherry"),
      (105, "South Branch", "Coimbatore")
    ).toDF("branch_id", "branch_name", "city")

    println("BRANCH MASTER")
    branches.show()

    // Large fact DataFrame: 1 million transactions
    val transactions = spark.range(1, 1000001)
      .select(
        col("id").as("transaction_id"),
        (pmod(col("id"), lit(5)) + 101)
          .cast("int").as("branch_id"),
        (col("id") * 100 % 50000 + 500)
          .cast("double").as("amount")
      )

    println("TRANSACTION COUNT")
    println(transactions.count())

    // Inspect the size of each dataset
    println("BRANCH COUNT")
    println(branches.count())

    // Disable automatic broadcasting for the
    // non-broadcast comparison
    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold", "-1"
    )

    // 1. Regular join
    val regularJoin = transactions.join(
      branches,
      Seq("branch_id"),
      "inner"
    )

    println("REGULAR JOIN PLAN")
    regularJoin.explain(true)

    // 2. Explicit broadcast join
    val broadcastJoin = transactions.join(
      broadcast(branches),
      Seq("branch_id"),
      "inner"
    )

    println("BROADCAST JOIN PLAN")
    broadcastJoin.explain(true)

    // Display a few joined records
    println("BROADCAST JOIN RESULTS")
    broadcastJoin.show(10, truncate = false)

    // Count the joined records
    println("BROADCAST JOIN COUNT")
    println(broadcastJoin.count())

    spark.stop()
  }
}

