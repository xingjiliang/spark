package application

import application.GenerateFnnSampleV2.{LOG, continuousFeatureIds, droppedFeatureIds, sparkContext}
import configuration.CommandLineArgumentsConfiguration._
import configuration.SymbolConfiguration._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
import util.{ApplicationArguments, DateUtil, HdfsUtil}

import scala.collection.mutable.ArrayBuffer

object GenerateTfrecordFormatSample {

  val LOG = LoggerFactory.getLogger(GenerateTfrecordFormatSample.getClass)
  var sparkSession: SparkSession = null
  var businessCode: String = null
  var startDate: String = null
  var endDate: String = null
  var featureType: String = null
  var continuousFeatureIds: Array[String] = null
  var valueIndexMapFilePath: String = null

  val PREFIX = "/home/hdp_lbg_supin/resultdata/common_feature/offline"
  val LIBSVM_FEATURE = "feature"
  val DL_FEATURE = "dl_feature"

  val addCommandLineOptions = {
    ApplicationArguments.addOption("b", BUSINESS_CODE, true, "business code")
    ApplicationArguments.addOption("s", START_DATE, true, "start date")
    ApplicationArguments.addOption("e", END_DATE, true, "end date")
    ApplicationArguments.addOption("t", FEATURE_TYPE, true, "feature type,discrete or continuous or org")
    ApplicationArguments.addOption("c", CONTINUOUS_FEATURE_IDS, true, "因有缺失值需要另行添加flag的特征IDs")
    ApplicationArguments.addOption("v", VALUE_INDEX_MAP_FILE, true, "value index map file name")
  }

  def parseArgs(args: Array[String]): Boolean = {
    val commandLine = ApplicationArguments.parseArgsToCommandLine(args)
    if (!ApplicationArguments.hasAllRequiredOptions(commandLine,
      Set(BUSINESS_CODE,
        START_DATE,
        END_DATE,
        FEATURE_TYPE,
        CONTINUOUS_FEATURE_IDS,
        VALUE_INDEX_MAP_FILE))) {
      return false
    }
    businessCode = commandLine.getOptionValue(BUSINESS_CODE)
    startDate = commandLine.getOptionValue(START_DATE)
    endDate = commandLine.getOptionValue(END_DATE)
    featureType = commandLine.getOptionValue(FEATURE_TYPE)
    continuousFeatureIds = commandLine.getOptionValue(CONTINUOUS_FEATURE_IDS).split(COMMA)
    valueIndexMapFilePath = commandLine.getOptionValue(VALUE_INDEX_MAP_FILE)
    return true
  }

  def main(args: Array[String]) = {
    if (!parseArgs(args)) {
      System.exit(1)
    }
    sparkSession = SparkSession.builder().getOrCreate()

    val featureIdValueIndexMap: Map[String, Map[String, String]] = GenerateFnnSampleV2.getFeatureIdValueIndexMap()
    val broadCastFeatureIdValueIndexMap = sparkSession.sparkContext.broadcast(featureIdValueIndexMap)
    val broadCastContinuousFeatureIds = sparkSession.sparkContext.broadcast(continuousFeatureIds)
    val broadCastDroppedFeatureIds = sparkSession.sparkContext.broadcast(droppedFeatureIds)
    val schema = StructType(
      List(
        StructField("test", IntegerType)
      )
    )

    DateUtil.getDateIntervalDays(startDate, endDate).foreach(dateString => {
      val outputDataPath = List(PREFIX, businessCode, DL_FEATURE, dateString, featureType).mkString(SLASH)
      HdfsUtil.deleteIfExists(sparkSession.sparkContext.hadoopConfiguration, outputDataPath)
      val inputDataPath = List(PREFIX, businessCode, LIBSVM_FEATURE, dateString, featureType).mkString(SLASH)
      sparkSession.createDataFrame(sparkSession.sparkContext.textFile(inputDataPath).repartition(256).mapPartitions(iterator => {
        val featureIdValueIndexMap = broadCastFeatureIdValueIndexMap.value
        val continuousFeatureIds = broadCastContinuousFeatureIds.value
        val droppedFeatureIds = broadCastDroppedFeatureIds.value.get
        iterator.map(line => {
          transformLibsvmDatasetToRow(line, featureIdValueIndexMap, continuousFeatureIds, droppedFeatureIds)
        }).filter(row => null != row)
      }), schema).write.format("tfrecords").option("recordType", "Example").save(outputDataPath)
    })
  }

  /**
    *
    * @param line 99001277211568-54310374058518app-app_index_gz37929462106500129586471205920840201106376 0	3102:0	3108:0	3114:0
    * @return
    */
  def transformLibsvmDatasetToRow(line: String,
                                  featureIdValueIndexMap: Map[String, Map[String, String]],
                                  continuousFeatureIds: Array[String],
                                  droppedFeatureIds: Array[String]): Row = {
    var flag = true
    val fields = line.split("\t")
    val label = fields(1)
    var valueIsDefaultArray = ArrayBuffer[String]()
    val featureValues = fields.drop(2).map(field => {
      val setIdAndValue = field.split(":")
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
            flag = false
          }
        }
      }
      if (droppedFeatureIds != None && !droppedFeatureIds.contains(setId)) {
        value
      } else {
        null
      }
    }).filter(value => {
      value != null
    })
    if (flag) {
      new GenericRow((featureValues ++: valueIsDefaultArray :+ label).toArray[Any])
    } else {
      null
    }
  }

}
