import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler._

class ExecutionListener extends SparkListener {

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println()
    println(">>> JOB STARTED")
    println(s"Job ID: ${jobStart.jobId}")
    println(s"Stage IDs: ${jobStart.stageInfos.map(_.stageId).mkString(", ")}")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val info = stageSubmitted.stageInfo
    println()
    println(">>> STAGE STARTED")
    println(s"Stage ID: ${info.stageId}")
    println(s"Stage Name: ${info.name}")
    println(s"Number of Tasks: ${info.numTasks}")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println(
      s">>> TASK COMPLETED | Stage: ${taskEnd.stageId} | " +
      s"Task: ${taskEnd.taskInfo.taskId} | " +
      s"Partition: ${taskEnd.taskInfo.index}"
    )
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println()
    println(">>> STAGE COMPLETED")
    println(s"Stage ID: ${stageCompleted.stageInfo.stageId}")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println()
    println(">>> JOB COMPLETED")
    println(s"Job ID: ${jobEnd.jobId}")
    println(s"Result: ${jobEnd.jobResult}")
  }
}

object Day8DAG {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day8-DAG-Spark-Execution")
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    sc.addSparkListener(new ExecutionListener)

    println()
    println("==============================================")
    println("       DAY 8 - DAG AND SPARK EXECUTION")
    println("==============================================")

    // INPUT
    val lines = sc.textFile("data/sales.txt", 4)

    println()
    println("INPUT")
    println("----------------------------------------------")
    println(s"Input partitions: ${lines.getNumPartitions}")

    // NARROW TRANSFORMATION 1
    val parsed = lines.map(_.split(" "))

    // NARROW TRANSFORMATION 2
    val validRecords =
      parsed.filter(parts => parts.length == 2)

    // NARROW TRANSFORMATION 3
    val pairs =
      validRecords.map(parts => (parts(0), parts(1).toInt))

    // WIDE TRANSFORMATION
    // SHUFFLE HAPPENS HERE
    val totals =
      pairs.reduceByKey(_ + _)

    // NARROW TRANSFORMATION AFTER SHUFFLE
    val finalResult =
      totals.map {
        case (product, quantity) =>
          s"$product -> $quantity"
      }

    println()
    println("TRANSFORMATION PIPELINE")
    println("----------------------------------------------")
    println("1. textFile()")
    println("2. map()        -> NARROW")
    println("3. filter()     -> NARROW")
    println("4. map()        -> NARROW")
    println("5. reduceByKey()-> WIDE / SHUFFLE")
    println("6. map()        -> NARROW")
    println("7. collect()    -> ACTION")

    println()
    println("SHUFFLE BOUNDARY")
    println("----------------------------------------------")
    println("reduceByKey() creates a shuffle.")
    println("Therefore the main pipeline has 2 stages:")
    println("Stage 0 -> transformations BEFORE shuffle")
    println("Stage 1 -> transformations AFTER shuffle")

    println()
    println("RDD LINEAGE")
    println("----------------------------------------------")
    println(finalResult.toDebugString)

    // ACTION 1
    println()
    println("==============================================")
    println("             ACTION 1: COLLECT")
    println("==============================================")

    val result = finalResult.collect()

    println()
    println("FINAL RESULT")
    println("----------------------------------------------")

    result.sorted.foreach(println)

    // ACTION 2
    println()
    println("==============================================")
    println("              ACTION 2: COUNT")
    println("==============================================")

    val count = finalResult.count()

    println(s"Number of final records: $count")

    // PARTITIONS
    println()
    println("==============================================")
    println("             PARTITION INFORMATION")
    println("==============================================")

    println(s"Input partitions : ${lines.getNumPartitions}")
    println(s"Pairs partitions : ${pairs.getNumPartitions}")
    println(s"Final partitions : ${finalResult.getNumPartitions}")

    println()
    println("==============================================")
    println("              DAY 8 SUMMARY")
    println("==============================================")

    println("Jobs are created by actions.")
    println("Stages are separated by shuffle boundaries.")
    println("Tasks execute work on partitions.")
    println("map() and filter() are narrow transformations.")
    println("reduceByKey() is a wide transformation.")
    println("reduceByKey() creates a shuffle.")
    println("Main reduceByKey pipeline = 2 stages.")

    println()
    println("==============================================")
    println("             APPLICATION COMPLETE")
    println("==============================================")

    sc.stop()
  }
}
