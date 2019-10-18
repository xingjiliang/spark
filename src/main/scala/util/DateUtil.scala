package util

import java.text.{DateFormat, SimpleDateFormat}
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object DateUtil {
  def getDateIntervalDays(startDate: String, endDate: String): Array[String] = {
    val dateFormat: DateFormat = new SimpleDateFormat("yyyyMMdd")
    val startLocalDateTime = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(startDate).toLocalDateTime
    val dates = mutable.ArrayBuffer[String]()
    breakable {
      var currDate = ""
      for (idx <- 0 to Int.MaxValue) {
        currDate = dateFormat.format((startLocalDateTime.plusDays(idx).toDate))
        dates += currDate
        if (currDate.equals(endDate)) break
      }
    }
    dates.toArray
  }

  def main(args:Array[String]): Unit = {
    getDateIntervalDays("20190610", "20190617").foreach(dateString => {
      println(dateString)
    })
  }
}
