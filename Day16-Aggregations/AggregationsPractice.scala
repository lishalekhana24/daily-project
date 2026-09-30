import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsPractice {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day16 Aggregations Practice")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // --------------------------------------------------
    // Employee Salary Data
    // --------------------------------------------------

    val employees = Seq(
      ("Lisha", "IT", "Developer", 60000),
      ("Anu", "IT", "Developer", 70000),
      ("Rahul", "IT", "Manager", 90000),
      ("Priya", "HR", "Recruiter", 50000),
      ("Kiran", "HR", "Manager", 65000),
      ("Arun", "Finance", "Analyst", 55000),
      ("Meena", "Finance", "Manager", 80000),
      ("Vijay", "Sales", "Executive", 45000),
      ("Divya", "Sales", "Manager", 75000),
      ("Ravi", "Sales", "Executive", 50000)
    ).toDF("name", "department", "job", "salary")

    println("========== EMPLOYEE DATA ==========")
    employees.show()

    // --------------------------------------------------
    // 1. Basic Aggregations
    // --------------------------------------------------

    println("========== BASIC AGGREGATIONS ==========")

    employees.select(
      count("*").alias("employee_count"),
      sum("salary").alias("total_salary"),
      avg("salary").alias("average_salary"),
      min("salary").alias("minimum_salary"),
      max("salary").alias("maximum_salary")
    ).show()

    // --------------------------------------------------
    // 2. Department-wise Salary Statistics
    // --------------------------------------------------

    println("========== DEPARTMENT-WISE SALARY STATISTICS ==========")

    employees
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        sum("salary").alias("total_salary"),
        avg("salary").alias("average_salary"),
        min("salary").alias("minimum_salary"),
        max("salary").alias("maximum_salary")
      )
      .show()

    // --------------------------------------------------
    // 3. GroupBy with Multiple Columns
    // --------------------------------------------------

    println("========== GROUP BY DEPARTMENT AND JOB ==========")

    employees
      .groupBy("department", "job")
      .agg(
        count("*").alias("employee_count"),
        avg("salary").alias("average_salary"),
        sum("salary").alias("total_salary")
      )
      .show()

    // --------------------------------------------------
    // 4. HAVING-like Filtering
    // --------------------------------------------------

    println("========== DEPARTMENTS WITH AVERAGE SALARY > 60000 ==========")

    employees
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        avg("salary").alias("average_salary"),
        sum("salary").alias("total_salary")
      )
      .filter($"average_salary" > 60000)
      .show()

    // --------------------------------------------------
    // Hospital Department Revenue Data
    // --------------------------------------------------

    val hospital = Seq(
      ("Cardiology", "Chennai", 120, 250000),
      ("Cardiology", "Chennai", 100, 210000),
      ("Neurology", "Chennai", 80, 180000),
      ("Neurology", "Chennai", 90, 200000),
      ("Orthopedics", "Chennai", 110, 190000),
      ("Orthopedics", "Chennai", 95, 170000),
      ("Pediatrics", "Chennai", 130, 160000),
      ("Pediatrics", "Chennai", 125, 155000),
      ("Cardiology", "Pondicherry", 70, 140000),
      ("Neurology", "Pondicherry", 60, 125000)
    ).toDF(
      "department",
      "location",
      "patients",
      "revenue"
    )

    println("========== HOSPITAL DATA ==========")
    hospital.show()

    // --------------------------------------------------
    // 5. Hospital Department Revenue Metrics
    // --------------------------------------------------

    println("========== HOSPITAL DEPARTMENT REVENUE ==========")

    hospital
      .groupBy("department")
      .agg(
        count("*").alias("records"),
        sum("patients").alias("total_patients"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_revenue"),
        min("revenue").alias("minimum_revenue"),
        max("revenue").alias("maximum_revenue")
      )
      .show()

    // --------------------------------------------------
    // 6. Hospital Department + Location
    // --------------------------------------------------

    println("========== DEPARTMENT AND LOCATION REVENUE ==========")

    hospital
      .groupBy("department", "location")
      .agg(
        sum("patients").alias("total_patients"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_revenue")
      )
      .show()

    // --------------------------------------------------
    // 7. HAVING-like Hospital Filtering
    // --------------------------------------------------

    println("========== DEPARTMENTS WITH REVENUE > 350000 ==========")

    hospital
      .groupBy("department")
      .agg(
        sum("revenue").alias("total_revenue"),
        sum("patients").alias("total_patients")
      )
      .filter($"total_revenue" > 350000)
      .show()

    spark.stop()
  }
}
