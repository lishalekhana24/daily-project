object Day1ScalaEssentials {

  // --------------------------------------------------
  // 1. val, var and lazy val
  // --------------------------------------------------

  def demonstrateVariables(): Unit = {

    println("===== 1. VARIABLES =====")

    val collegeName = "ABC College"
    var studentCount = 3

    println(s"College Name: $collegeName")
    println(s"Initial Student Count: $studentCount")

    studentCount = 4

    println(s"Updated Student Count: $studentCount")

    lazy val message = {
      println("lazy val is being evaluated now")
      "This value is calculated only when needed"
    }

    println("Before accessing lazy val")
    println(message)

    println()
  }


  // --------------------------------------------------
  // 2. Immutable collections
  // --------------------------------------------------

  def demonstrateImmutableCollections(): Unit = {

    println("===== 2. IMMUTABLE COLLECTIONS =====")

    val numbers = List(10, 20, 30)

    println(s"Original List: $numbers")

    val updatedNumbers = numbers :+ 40

    println(s"New List: $updatedNumbers")
    println(s"Original List after operation: $numbers")

    println()
  }


  // --------------------------------------------------
  // 3. List, Vector, Set and Map
  // --------------------------------------------------

  def demonstrateCollections(): Unit = {

    println("===== 3. SCALA COLLECTIONS =====")

    // List
    val studentList = List("Alice", "Bob", "Charlie")

    println(s"List: $studentList")
    println(s"First student: ${studentList.head}")

    // Vector
    val marksVector = Vector(85, 90, 78, 92)

    println(s"Vector: $marksVector")
    println(s"Second mark: ${marksVector(1)}")

    // Set
    val subjects = Set("Scala", "Spark", "Scala", "SQL")

    println(s"Set: $subjects")

    // Map
    val studentMarks = Map(
      "Alice" -> 85,
      "Bob" -> 72,
      "Charlie" -> 91
    )

    println(s"Map: $studentMarks")
    println(s"Alice's mark: ${studentMarks("Alice")}")

    println()
  }


  // --------------------------------------------------
  // 4. For-comprehension with yield
  // --------------------------------------------------

  def demonstrateForComprehension(): Unit = {

    println("===== 4. FOR-COMPREHENSION WITH YIELD =====")

    val students = List(
      ("Alice", 85),
      ("Bob", 72),
      ("Charlie", 91),
      ("David", 65)
    )

    val passedStudents = for {
      (name, marks) <- students
      if marks >= 70
    } yield (name, marks)

    println("Students who scored 70 or above:")

    passedStudents.foreach {
      case (name, marks) =>
        println(s"$name -> $marks")
    }

    println()
  }


  // --------------------------------------------------
  // 5. Logger trait
  // --------------------------------------------------

  trait Logger {

    def log(message: String): Unit = {
      println(s"[LOG] $message")
    }
  }


  class StudentProcessor extends Logger {

    def process(): Unit = {
      log("Student processing started")
      log("Processing student grades")
    }
  }


  class GradeReporter extends Logger {

    def report(): Unit = {
      log("Grade report generated")
    }
  }


  // --------------------------------------------------
  // 6. Student Grade Processor
  // --------------------------------------------------

  case class Student(
    name: String,
    marks: Int
  )


  def calculateGrade(marks: Int): String = {

    if (marks >= 90) {
      "A"
    } else if (marks >= 80) {
      "B"
    } else if (marks >= 70) {
      "C"
    } else if (marks >= 60) {
      "D"
    } else {
      "F"
    }
  }


  def processStudentGrades(): Unit = {

    println("===== 6. STUDENT GRADE PROCESSOR =====")

    val students = List(
      Student("Alice", 95),
      Student("Bob", 82),
      Student("Charlie", 74),
      Student("David", 58),
      Student("Emma", 91)
    )

    val studentGrades = students.map { student =>
      val grade = calculateGrade(student.marks)
      (student.name, student.marks, grade)
    }

    println("Student Grade Report:")

    studentGrades.foreach {
      case (name, marks, grade) =>
        println(s"$name -> Marks: $marks -> Grade: $grade")
    }

    val passedStudents = studentGrades.filter {
      case (_, marks, _) => marks >= 60
    }

    println()
    println("Students who passed:")

    passedStudents.foreach {
      case (name, marks, grade) =>
        println(s"$name -> Marks: $marks -> Grade: $grade")
    }

    val averageMarks =
      students.map(_.marks).sum.toDouble / students.size

    println()
    println(f"Average Marks: $averageMarks%.2f")

    println()
  }


  // --------------------------------------------------
  // Main method
  // --------------------------------------------------

  def main(args: Array[String]): Unit = {

    println("==========================================")
    println("       SCALA DAY 1 ESSENTIALS")
    println("==========================================")
    println()

    demonstrateVariables()

    demonstrateImmutableCollections()

    demonstrateCollections()

    demonstrateForComprehension()

    println("===== 5. TRAIT AND IMPLEMENTATIONS =====")

    val processor = new StudentProcessor()
    processor.process()

    val reporter = new GradeReporter()
    reporter.report()

    println()

    processStudentGrades()

    println("==========================================")
    println("             PROGRAM COMPLETE")
    println("==========================================")
  }
}
