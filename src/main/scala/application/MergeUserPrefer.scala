package application

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import util.ApplicationArguments

import scala.collection.Map

object MergeUserPrefer {
  val LOG = LoggerFactory.getLogger("debugger")
  var sparkSession: SparkSession = null

  val query = "select"

  def _addOptions() = {
    ApplicationArguments.addOption("d", "date", true, "date")
    ApplicationArguments.addOption("f", "features", true, "feature names")
  }

  def main(args: Array[String]) = {
    _addOptions
    ApplicationArguments.parseArgsToCommandLine(args)
    sparkSession = SparkSession.builder().appName("check_user_prefer").enableHiveSupport.getOrCreate

    val date = ApplicationArguments.commandLine.getOptionValue("date")
    val featureArray = ApplicationArguments.commandLine.getOptionValue("features").split(",")
    jobStart(date, featureArray)
//      .saveAsTextFile(ApplicationArguments.commandLine.getOptionValue(OUTPUT_DATA_PATH))
  }

  /**
    * 与rank2.0相同的解析逻辑,把user id和出错feature name打出来
    */
  def jobStart(date: String, featureArray: Array[String]) = {
//    val errors = ArrayBuffer[String]()

    sparkSession.sql(getQuery(date, featureArray)).rdd.flatMap(row => {
      val (userId, featureValueMap) = (row.getString(0), row.getMap[String, String](1))
        var clearParams = scala.collection.Map[String, String]()
        clearParams ++= cleanParams(featureValueMap)
        if (null != clearParams && !clearParams.isEmpty && null != userId && userId.length > 3) {
          // 用户偏好打散
          featureArray.foreach { preferFeature =>
            val prefer = clearParams.get(preferFeature)
            // prefer Some("1_2|2_4|3_5")
            //println("preferFeature:" + preferFeature + "; prefer :" + prefer)
            if (prefer.isDefined) {
              prefer.get.split("\\|").map { p =>
                try {
                  val tmpArr = p.split("_", 2)
                  clearParams += ((s"${preferFeature}:${tmpArr(0)}", tmpArr(1)))
                } catch {
                  case e: Exception => {
                    LOG.error(s"user id:${userId}, error prefer:${prefer}, featureValueMap:${featureValueMap}\n", e)
//                    errors += s"user id:${userId}, error prefer:${prefer}, featureValueMap:${featureValueMap}"
                  }
                }
              }
              clearParams += ((preferFeature, "0"))
            }
          }
          Some(s"${userId}\t${clearParams}")
        } else {
          None
        }
    })
  }

  def getQuery(date: String, featureArray: Array[String]): String = {
    val paramMapQuery = featureArray.map(featureName => {
      s"'${featureName}', params['${featureName}']"
    }).mkString("map(", ", ", ")")
    val query = s"select id user_id, ${
      paramMapQuery
    } from hdp_lbg_supin_zplisting.t_feature_statisticsinformation_user where dt='${date}'"
    query
  }

  def cleanParams(featureMap: Map[String, String]) = {
    featureMap.filter{case (feature, param) => !isEmpty(param)}
    featureMap.flatMap {
      case (key, value) =>
        if (isEmpty(value)) {
          Some(key, value)
        } else {
          None
        }
    }
  }

  def isEmpty(str: String): Boolean = {
    str == null || str.trim.length == 0 || str.trim == "-" || str.trim == "null" || str.trim.isEmpty || str.trim == "\\N"
  }
}
