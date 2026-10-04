
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18-Joins")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Customers DataFrame
    val customers = Seq(
      (1, "Lisha", "Chennai"),
      (2, "Priya", "Bangalore"),
      (3, "Arun", "Hyderabad"),
      (4, "Ravi", "Puducherry")
    ).toDF("customer_id", "name", "city")

    // Orders DataFrame
    val orders = Seq(
      (101, 1, 2000),
      (102, 2, 3500),
      (103, 5, 1500),
      (104, 1, 4000)
    ).toDF("order_id", "customer_id", "amount")

    // Payments DataFrame
    val payments = Seq(
      (501, 101, "Paid"),
      (502, 102, "Pending"),
      (503, 104, "Paid"),
      (504, 105, "Paid")
    ).toDF("payment_id", "order_id", "status")

    println("CUSTOMERS")
    customers.show()

    println("ORDERS")
    orders.show()

    println("PAYMENTS")
    payments.show()

    spark.stop()
  }
}

