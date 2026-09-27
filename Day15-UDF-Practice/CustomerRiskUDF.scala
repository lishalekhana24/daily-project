import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustomerRiskUDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Customer Risk UDF")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Customer transaction data
    val transactions = Seq(
      ("C001", 500),
      ("C002", 2500),
      ("C003", 7500),
      ("C004", 12000),
      ("C005", 1500)
    ).toDF("customer_id", "transaction_value")

    println("Original Transaction Data:")
    transactions.show()

    // Customer risk UDF
    val riskCategory = udf((amount: Int) => {

      if (amount < 1000)
        "Low Risk"
      else if (amount < 5000)
        "Medium Risk"
      else
        "High Risk"

    })

    // Add calculated risk_category column
    val result = transactions.withColumn(
      "risk_category",
      riskCategory(col("transaction_value"))
    )

    println("Customer Risk Classification:")
    result.show()

    // Register UDF
    spark.udf.register("customer_risk_udf", (amount: Int) => {

      if (amount < 1000)
        "Low Risk"
      else if (amount < 5000)
        "Medium Risk"
      else
        "High Risk"

    })

    // Register DataFrame as temporary SQL view
    transactions.createOrReplaceTempView("transactions")

    // Execute SQL using registered UDF
    val sqlResult = spark.sql("""
      SELECT
        customer_id,
        transaction_value,
        customer_risk_udf(transaction_value) AS risk_category
      FROM transactions
    """)

    println("Risk Classification using Spark SQL:")
    sqlResult.show()

    spark.stop()
  }
}
