package application


import configuration.CommandLineArgumentsConfiguration._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import util.ApplicationArguments.commandLine
import util.{ApplicationArguments, DateUtil}

/**
  * 对样本进行处理，删除指定特征
  */
object DropSampleFeatures {

  private val LOG = LoggerFactory.getLogger("test")

  // 注意我又把这里改回来了
  private var sparkConf:SparkConf = null
  private var sparkContext:SparkContext = null

  val addCommandLineOptions = {
    ApplicationArguments.addOption("s", START_DATE, true, "start date")
    ApplicationArguments.addOption("e", END_DATE, true, "end date")
    ApplicationArguments.addOption("d", "drop-features", true, "droppedFeatureList")
    ApplicationArguments.addOption("t", "type", true, "type")
    ApplicationArguments.addOption("k", "keep-features", true, "keepFeatureList")
  }

  def main(args: Array[String]): Unit = {
    ApplicationArguments.parseArgs(args)
    if (!ApplicationArguments.hasAllRequiredOptions(List(INPUT_DATA_PATH, OUTPUT_DATA_PATH, START_DATE, END_DATE, "type"))) {
      sys.exit(1)
    }
    sparkConf = new SparkConf().setAppName("spark-demo-98587")
    sparkContext = new SparkContext(sparkConf)
    jobStart()
  }

  def jobStart(): Unit = {
    var keepFlag: Boolean = true
    val features = if (null != commandLine.getOptionValue('d')) {
      keepFlag = false
      commandLine.getOptionValue('d')
    } else {
      commandLine.getOptionValue('k')
    }
    val keepFlagBroadcastValue = sparkContext.broadcast(keepFlag)
    val featuresBroadcastValue = sparkContext.broadcast(features)
    DateUtil.getDateIntervalDays(commandLine.getOptionValue(START_DATE), commandLine.getOptionValue(END_DATE))
      .foreach(dateString => {
        sparkContext.textFile(commandLine.getOptionValue(INPUT_DATA_PATH) + "/" + dateString + "/" + commandLine.getOptionValue("type")).repartition(512).mapPartitions(iterator => {
          val featureArray = featuresBroadcastValue.value.split(",")
          iterator.map(line => {
            val splitLineList = line.split("\t")
            val header = splitLineList(0)
            val label = splitLineList(1)
            val cleanedFeatureIdValueMapArray = splitLineList.drop(2).filter(featureIdValueMap => {
              if (keepFlagBroadcastValue.value) {
                (featureArray.contains(featureIdValueMap.split(":")(0).trim()))
              } else {
                !(featureArray.contains(featureIdValueMap.split(":")(0).trim()))
              }
            })
            header + "\t" + label + "\t" + cleanedFeatureIdValueMapArray.mkString("\t")
          })
        }).repartition(4).saveAsTextFile(commandLine.getOptionValue(OUTPUT_DATA_PATH) + "/" + dateString + "/" + commandLine.getOptionValue("type"))
    })
  }

}