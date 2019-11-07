package util

import java.text.{DateFormat, SimpleDateFormat}

import configuration.SymbolConfiguration.SLASH
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object DateUtil {
  def getDateIntervalDays(startDate: String, endDate: String): Array[String] = {
    val dateFormat: DateFormat = new SimpleDateFormat("yyyyMMdd")
    val startLocalDateTime = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(startDate).toLocalDateTime
    val dates = mutable.ArrayBuffer[String]()
    breakable {
      var currentDate = ""
      for (idx <- 0 to Int.MaxValue) {
        currentDate = dateFormat.format((startLocalDateTime.plusDays(idx).toDate))
        dates += currentDate
        if (currentDate.equals(endDate)) break
      }
    }
    dates.toArray
  }

  def main(args:Array[String]): Unit = {
    getDateIntervalDays("20190610", "20190610").foreach(dateString => {
      println(dateString)
    })
    val inputDataPath = "/home/hdp_lbg_supin/resultdata/common_feature/offline/dl_features_sample_test"
    val featureType = "org"
    println(List(inputDataPath, "20191008", featureType).mkString(SLASH))
  }
}
