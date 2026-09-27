import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SalaryUDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day15-UDF-Practice")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Employee salary data
    val employees = Seq(
      ("Alice", 25000),
      ("Bob", 45000),
      ("Charlie", 75000),
      ("David", 120000),
      ("Eva", 55000)
    ).toDF("name", "salary")

    println("Original Employee Data:")
    employees.show()

    // Scala UDF for salary classification
    val salaryBand = udf((salary: Int) => {
      if (salary < 30000)
        "Low"
      else if (salary < 70000)
        "Medium"
      else
        "High"
    })

    // Add calculated column using withColumn
    val result = employees.withColumn(
      "salary_band",
      salaryBand(col("salary"))
    )

    println("Salary Classification using UDF:")
    result.show()

    // Built-in Spark function comparison
    val builtInResult = employees.withColumn(
      "salary_band",
      when(col("salary") < 30000, "Low")
        .when(col("salary") < 70000, "Medium")
        .otherwise("High")
    )

    println("Salary Classification using Built-in Function:")
    builtInResult.show()

    // Register UDF with Spark SQL
    spark.udf.register("salary_band_udf", (salary: Int) => {
      if (salary < 30000)
        "Low"
      else if (salary < 70000)
        "Medium"
      else
        "High"
    })

    // Create temporary view
    employees.createOrReplaceTempView("employees")

    // Use registered UDF in Spark SQL
    val sqlResult = spark.sql("""
      SELECT
        name,
        salary,
        salary_band_udf(salary) AS salary_band
      FROM employees
    """)

    println("Salary Classification using Registered UDF:")
    sqlResult.show()

    spark.stop()
  }
}
