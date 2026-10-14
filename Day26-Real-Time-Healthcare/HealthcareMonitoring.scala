
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import scala.collection.mutable

case class VitalEvent(
    patientId: String,
    eventTime: String,
    heartRate: Double,
    spo2: Double,
    temperature: Double,
    systolicBP: Int
)

case class VitalThresholds(
    minHeartRate: Double = 60.0,
    maxHeartRate: Double = 100.0,
    minSpO2: Double = 95.0,
    maxTemperature: Double = 38.0,
    maxSystolicBP: Int = 140
)

object HealthcareMonitoring {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day26-Real-Time-Healthcare")
      .setMaster("local[2]")
      .set("spark.serializer",
        "org.apache.spark.serializer.JavaSerializer")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.sparkContext.setLogLevel("OFF")
    ssc.checkpoint("./checkpoint")

    val thresholds =
      ssc.sparkContext.broadcast(VitalThresholds())

    val abnormalCounter =
      ssc.sparkContext.longAccumulator("AbnormalVitalReadings")

    val batches = Seq(
      Seq(
        VitalEvent("P101", "10:00:00", 75, 98, 36.8, 120),
        VitalEvent("P102", "10:00:00", 125, 89, 37.0, 130)
      ),
      Seq(
        VitalEvent("P101", "10:00:02", 78, 98, 36.9, 122),
        VitalEvent("P102", "10:00:02", 130, 90, 37.1, 132)
      ),
      Seq(
        VitalEvent("P101", "10:00:04", 80, 97, 36.8, 125),
        VitalEvent("P102", "10:00:04", 128, 91, 37.2, 135)
      ),
      Seq(
        VitalEvent("P101", "10:00:06", 115, 94, 38.5, 145),
        VitalEvent("P102", "10:00:06", 82, 98, 36.8, 125)
      ),
      Seq(
        VitalEvent("P101", "10:00:08", 120, 92, 38.7, 150),
        VitalEvent("P102", "10:00:08", 85, 97, 36.9, 128)
      ),
      Seq(
        VitalEvent("P101", "10:00:10", 122, 91, 38.8, 152),
        VitalEvent("P102", "10:00:10", 80, 98, 36.8, 126)
      ),
      Seq(
        VitalEvent("P101", "10:00:12", 82, 98, 36.9, 124),
        VitalEvent("P102", "10:00:12", 118, 93, 38.2, 145)
      ),
      Seq(
        VitalEvent("P101", "10:00:14", 79, 98, 36.8, 122),
        VitalEvent("P102", "10:00:14", 121, 92, 38.4, 148)
      )
    )

    val queue = mutable.Queue[RDD[VitalEvent]]()

    batches.foreach { batch =>
      queue.enqueue(ssc.sparkContext.parallelize(batch))
    }

    val patientStream = ssc.queueStream(queue, oneAtATime = true)

    val checkedReadings = patientStream.map { event =>
      val t = thresholds.value

      val reasons = Seq(
        if (event.heartRate < t.minHeartRate ||
            event.heartRate > t.maxHeartRate)
          Some("Abnormal heart rate") else None,

        if (event.spo2 < t.minSpO2)
          Some("Low SpO2") else None,

        if (event.temperature > t.maxTemperature)
          Some("High temperature") else None,

        if (event.systolicBP > t.maxSystolicBP)
          Some("High systolic blood pressure") else None
      ).flatten

      val abnormal = reasons.nonEmpty

      if (abnormal) {
        abnormalCounter.add(1L)
      }

      (event, abnormal, reasons.mkString(", "))
    }

    checkedReadings.foreachRDD { rdd =>
      rdd.collect().foreach {
        case (event, abnormal, reasons) =>
          println(
            s"[VITAL] Patient=${event.patientId}, " +
            s"HR=${event.heartRate}, SpO2=${event.spo2}, " +
            s"Temp=${event.temperature}, BP=${event.systolicBP}"
          )

          if (abnormal) {
            println(
              s"[ALERT] Patient ${event.patientId}: $reasons"
            )
          }
      }
    }

    val abnormalOnly = checkedReadings.filter(_._2)

    abnormalOnly
      .countByWindow(Seconds(10), Seconds(4))
      .foreachRDD { rdd =>
        rdd.collect().foreach { count =>
          println(s"[WINDOW] Abnormal readings in window: $count")
        }
      }

    val patientAbnormalState = checkedReadings
      .map { case (event, abnormal, _) =>
        (event.patientId, abnormal)
      }
      .updateStateByKey[Int](
        (values: Seq[Boolean], previous: Option[Int]) => {
          val startingCount = previous.getOrElse(0)

          val newCount = values.foldLeft(startingCount) {
            (count, abnormal) =>
              if (abnormal) count + 1 else 0
          }

          Some(newCount)
        }
      )

    patientAbnormalState.foreachRDD { rdd =>
      rdd.collect().foreach { case (patientId, count) =>
        println(
          s"[STATE] Patient=$patientId, " +
          s"consecutive abnormal readings=$count"
        )

        if (count >= 2) {
          println(
            s"[REPEATED ALERT] Patient $patientId " +
            "has repeated abnormal readings"
          )
        }
      }
    }

    println("Starting Real-Time Healthcare Monitoring...")
    println("Batch interval: 2 seconds")
    println("Window: 10 seconds | Slide: 4 seconds")

    ssc.start()
    ssc.awaitTerminationOrTimeout(22000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)

    println(
      s"[SUMMARY] Abnormal readings processed: ${abnormalCounter.value}"
    )
  }
}

