package application

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import configuration.CommandLineArgumentsConfiguration.{VALUE_INDEX_MAP_FILE, _}
import configuration.SymbolConfiguration._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import util.{ApplicationArguments, DateUtil, HdfsUtil}

import scala.collection.JavaConverters
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 生成FNN样本，处理连续特征的缺省值，离散特征值1.如有缺省值+1；2.如值不连贯，令其连贯(即从0开始)；
  * 统计特征不做处理，不要交叉特征
  * 类别1: 连续特征0.0-1.0, 大于0过滤掉 -1.0转为0.0并添加指示
  * 类别2: 连续特征0、1、2，小于0过滤掉
  * 类别3: 离散特征0-1
  * 类别4: 离散特征，如分段年龄、时间、城市、类别、工作年限根据map转换
  */
object GenerateFnnSampleV2 {

  private val LOG = LoggerFactory.getLogger(GenerateFnnSampleV2.getClass)

  private var sparkConf: SparkConf = null
  private var sparkContext: SparkContext = null

  var inputDataPath: String = null
  var outputDataPath: String = null
  var startDate: String = null
  var endDate: String = null
  var featureType: String = null
  var continuousFeatureIds: Array[String] = null
  var droppedFeatureIds: Option[Array[String]] = null
  var valueIndexMapFilePath: String = null

  val addCommandLineOptions = {
    ApplicationArguments.addOption("s", START_DATE, true, "start date")
    ApplicationArguments.addOption("e", END_DATE, true, "end date")
    ApplicationArguments.addOption("t", FEATURE_TYPE, true, "feature type,discrete or continuous or org")
    ApplicationArguments.addOption("c", CONTINUOUS_FEATURE_IDS, true, "continuous feature ids")
    ApplicationArguments.addOption("d", DROPPED_FEATURE_IDS, true, "value index map file name")
    ApplicationArguments.addOption("v", VALUE_INDEX_MAP_FILE, true, "value index map file name")
  }

  def main(args: Array[String]): Unit = {
    if (!parseArgs(args)) {
      System.exit(1)
    }
    sparkConf = new SparkConf().setAppName(GenerateFnnSampleV2.getClass.toString)
    sparkContext = new SparkContext(sparkConf)
    jobStart()
  }

  def parseArgs(args: Array[String]): Boolean = {
    val commandLine = ApplicationArguments.parseArgsToCommandLine(args)
    if (!ApplicationArguments.hasAllRequiredOptions(commandLine,
      Set(INPUT_DATA_PATH,
        OUTPUT_DATA_PATH,
        START_DATE,
        END_DATE,
        FEATURE_TYPE,
        CONTINUOUS_FEATURE_IDS,
        VALUE_INDEX_MAP_FILE))) {
      return false
    }
    inputDataPath = commandLine.getOptionValue(INPUT_DATA_PATH)
    outputDataPath = commandLine.getOptionValue(OUTPUT_DATA_PATH)
    startDate = commandLine.getOptionValue(START_DATE)
    endDate = commandLine.getOptionValue(END_DATE)
    featureType = commandLine.getOptionValue(FEATURE_TYPE)
    continuousFeatureIds = commandLine.getOptionValue(CONTINUOUS_FEATURE_IDS).split(COMMA)
    valueIndexMapFilePath = commandLine.getOptionValue(VALUE_INDEX_MAP_FILE)
    droppedFeatureIds = {
      val droppedFeatureIdsString = commandLine.getOptionValue(DROPPED_FEATURE_IDS)
        if (null != droppedFeatureIdsString) {
          Some(droppedFeatureIdsString.split(COMMA))
        } else {
          None
        }
    }
    return true
  }

  def getFeatureIdValueIndexMap(): Map[String, Map[String, String]] = {
    val gson = new Gson()
    val JsonString = sparkContext.textFile(valueIndexMapFilePath).collect().mkString(LINE_FEED)
    JavaConverters.mapAsScalaMapConverter[String, java.util.Map[String, String]](
      gson.fromJson(JsonString, new TypeToken[java.util.Map[String, java.util.Map[String, String]]]() {}.getType)
    ).asScala.mapValues(value => {
      JavaConverters.mapAsScalaMapConverter(value).asScala.toMap
    }).toMap
  }

  def jobStart(): Unit = {
    val featureIdValueIndexMap: Map[String, Map[String, String]] = getFeatureIdValueIndexMap()
    val broadCastFeatureIdValueIndexMap = sparkContext.broadcast(featureIdValueIndexMap)
    val broadCastContinuousFeatureIds = sparkContext.broadcast(continuousFeatureIds)
    val broadCastDroppedFeatureIds = sparkContext.broadcast(droppedFeatureIds)

    DateUtil.getDateIntervalDays(startDate, endDate).foreach(dateString => {
      val finalOutputDataPath = List(outputDataPath, dateString, featureType).mkString(SLASH)
      HdfsUtil.deleteIfExists(sparkContext.hadoopConfiguration, finalOutputDataPath)

      sparkContext.textFile(List(inputDataPath, dateString, featureType).mkString(SLASH))
        .repartition(256).mapPartitions(iterator => {
        val featureIdValueIndexMap = broadCastFeatureIdValueIndexMap.value
        val continuousFeatureIds = broadCastContinuousFeatureIds.value
        val droppedFeatureIds = broadCastDroppedFeatureIds.value.get

        iterator.map(line => {
          var flag: ListBuffer[String] = ListBuffer()
          val splitLineArray = line.split("\t")
          val label = splitLineArray(1)
          var valueIsDefaultArray = ArrayBuffer[String]()
          val cleanedFeatureIdValueMapArray = splitLineArray.drop(2).map(featureIdValueJsonMapString => {
            val setIdAndValue = featureIdValueJsonMapString.split(":")
            var (setId, value) = (setIdAndValue(0).trim, setIdAndValue(1).trim)
            if (continuousFeatureIds.contains(setId)) {
              if ((value.toFloat + 1.0f) < 1e-4) {
                value = "0"
                valueIsDefaultArray += "1"
              } else {
                valueIsDefaultArray += "0"
              }
            } else if (featureIdValueIndexMap.contains(setId)) {
              try {
                value = featureIdValueIndexMap.get(setId).get.get(value).get
              } catch {
                case e: Exception => {
                  LOG.info("出现未知值.", e)
                  flag += setId
                }
              }
            }
            if (droppedFeatureIds != None && !droppedFeatureIds.contains(setId)) {
              value
            } else {
              None
            }
          }).filter(value => {
            value != None
          })
          if (flag.isEmpty) {
            None
          } else {
            (splitLineArray.drop(2) ++ flag).mkString("\t")
          }
        }).filter(line => {
          line != None
        })
      }).repartition(1).saveAsTextFile(finalOutputDataPath)
    })
  }

}