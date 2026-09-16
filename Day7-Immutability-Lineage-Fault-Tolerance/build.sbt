ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Day7-Immutability-Lineage-Fault-Tolerance",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.6"
    ),

    Compile / run / fork := true,

    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
