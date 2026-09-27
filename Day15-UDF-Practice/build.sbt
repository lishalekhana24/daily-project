ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Day15-UDF-Practice",
    version := "1.0",

    fork := true,

    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.6",
  "org.apache.spark" %% "spark-sql" % "3.5.6"
)
