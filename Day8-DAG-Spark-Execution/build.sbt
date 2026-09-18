ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Day8-DAG-Spark-Execution",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.6"
    ),

    Compile / run / fork := true,

    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
