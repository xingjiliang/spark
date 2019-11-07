package application

import configuration.CommandLineArgumentsConfiguration.OUTPUT_DATA_PATH
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import util.ApplicationArguments
import test.JavaUtil

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object CheckUserPrefer {
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
    jobStart(date, featureArray).saveAsTextFile(ApplicationArguments.commandLine.getOptionValue(OUTPUT_DATA_PATH))
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
          val errorPreferArray: ArrayBuffer[String] = ArrayBuffer()
          // 用户偏好打散
          featureArray.foreach { preferFeature =>
            val prefer = clearParams.get(preferFeature)
            // prefer Some("1_2|2_4|3_5")
            // println("preferFeature:" + preferFeature + "; prefer :" + prefer)
            if (prefer.isDefined) {
              prefer.get.split("\\|").map { p =>
                try {
                  val tmpArr = p.split("_", 2)
                  clearParams += ((s"${preferFeature}:${tmpArr(0)}", tmpArr(1)))
                } catch {
                  case e: Exception => {
                    LOG.error(s"user id:${userId}, error prefer:${prefer}, featureValueMap:${featureValueMap}\n", e)
                    errorPreferArray += preferFeature
                  }
                }
              }
              clearParams += ((preferFeature, "0"))
            } else {
              None
            }
          }
          if (errorPreferArray.nonEmpty) {
            Some(s"${userId}\t${errorPreferArray.mkString(",")}\t${featureValueMap}")
          } else {
            None
          }
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
    } from hdp_lbg_supin_zplisting.t_feature_statisticsinformation_user where dt='${date}' and batch=0"
    query
  }

  def cleanParams(featureMap: Map[String, String]) = {
    featureMap.flatMap {
      case (key, value) =>
        if (!JavaUtil.isEmpty(value)) {
          Some(key, value)
        } else {
          None
        }
    }
  }
}
