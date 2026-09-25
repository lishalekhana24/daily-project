import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

case class Employee(
  id: Int,
  name: String,
  department: String,
  salary: Double
)

object EmployeePayroll {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Employee Payroll Pipeline")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Employee data
    val employees = Seq(
      Employee(101, "Lisha", "IT", 50000),
      Employee(102, "Arun", "HR", 45000),
      Employee(103, "Priya", "IT", 60000),
      Employee(104, "Rahul", "Finance", 55000),
      Employee(105, "Anu", "HR", 48000)
    )

    // Create Dataset
    val employeeDS: Dataset[Employee] =
      spark.createDataset(employees)

    println("=== Dataset ===")
    employeeDS.show()

    // Dataset -> DataFrame
    val employeeDF: DataFrame =
      employeeDS.toDF()

    println("=== DataFrame ===")
    employeeDF.show()

    // Filter
    val highSalaryDF =
      employeeDF.filter($"salary" > 50000)

    println("=== Employees with Salary > 50000 ===")
    highSalaryDF.show()

    // DataFrame -> Dataset
    val employeeDS2: Dataset[Employee] =
      employeeDF.as[Employee]

    println("=== DataFrame converted back to Dataset ===")
    employeeDS2.show()

    // Payroll calculation
    val payrollDS = employeeDS.map { employee =>
      val annualSalary = employee.salary * 12

      (
        employee.id,
        employee.name,
        employee.department,
        employee.salary,
        annualSalary
      )
    }

    println("=== Employee Payroll ===")
    payrollDS.show()

    // Department-wise salary
    val departmentSalary =
      employeeDF
        .groupBy($"department")
        .sum("salary")

    println("=== Department-wise Salary ===")
    departmentSalary.show()

    spark.stop()
  }
}
