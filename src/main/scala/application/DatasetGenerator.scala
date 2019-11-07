package application

import configuration.CommandLineArgumentsConfiguration._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import util.ApplicationArguments

/**
  * 可以支持生成连续多天配置文件的样本
  */
object DatasetGenerator {
  val LOG = LoggerFactory.getLogger("debugger")
  var sparkSession: SparkSession = null
  var inputDataPath: String = null
  var outputDataPath: String = null
  var startDate: String = null
  var endDate: String = null
  var featureConfigFilesPath: String = null
  var xgboostModel: String = null

  // 以下路径原本存于特征配置文件，但如果这个app一次只能处理一个特征配置文件的话，那完全可以把特征配置文件中的部分信息提取到命令行参数中


  def _addOptions(): Unit = {
    ApplicationArguments.addOption("s", START_DATE, true, "start date")
    ApplicationArguments.addOption("e", END_DATE, true, "end date")
    ApplicationArguments.addOption("f", "feature", true, "featureconfig")
    ApplicationArguments.addOption("x", "xgboost", true, "xgboost model path")
  }

  def setGlobalVariables(): Boolean = {
    inputDataPath = ApplicationArguments.commandLine.getOptionValue(INPUT_DATA_PATH)
    outputDataPath = ApplicationArguments.commandLine.getOptionValue(OUTPUT_DATA_PATH)
    startDate = ApplicationArguments.commandLine.getOptionValue(START_DATE)
    endDate = ApplicationArguments.commandLine.getOptionValue(END_DATE)
    featureConfigFilesPath = ApplicationArguments.commandLine.getOptionValue("feature")
    xgboostModel = ApplicationArguments.commandLine.getOptionValue("xgboost")
    true
  }

  def getInitializedSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.referenceTracking", "false")
    //    sparkConf.registerKryoClasses(Array(classOf[String], classOf[Map[String, String]], classOf[FeatureGroup]))
    sparkConf.set("spark.rpc.message.maxSize", "512")
    sparkConf.set("spark.default.parallelism", "512")
    sparkConf.set("spark.yarn.executor.memoryOverhead", "8g")
    sparkConf.set("spark.shuffle.file.buffer", "64k")
    sparkConf.set("spark.reducer.maxSizeInFlight", "24m")
    sparkConf.set("spark.shuffle.io.maxRetries", "60")
    sparkConf.set("spark.shuffle.io.retryWait", "10s")
    sparkConf.set("spark.speculation", "true")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("spark.driver.maxResultSize", "5g")
    sparkConf.set("spark.hadoop.dfs.client.block.write.locateFollowingBlock.retries", "8")
    sparkConf.set("spark.kryoserializer.buffer.max", "2000m")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "64")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "256")
    sparkConf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "600s")
    sparkConf.set("spark.scheduler.listenerbus.eventqueue.size", "100000")
    sparkConf.set("spark.shuffle.io.preferDirectBufs", "false")
    sparkConf.set("spark.executor.extraJavaOptions", "-XX:MaxDirectMemorySize=2g -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60 -XX:+CMSParallelRemarkEnabled -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=31 -XX:+UseCompressedOops -XX:-UseGCOverheadLimit -XX:+CMSIncrementalMode -XX:+CMSIncrementalPacing -XX:CMSIncrementalDutyCycleMin=0 -XX:CMSIncrementalDutyCycle=10")
    sparkConf
  }

  def main(args: Array[String]): Unit = {
    _addOptions()
    ApplicationArguments.parseArgsToCommandLine(args)
    setGlobalVariables()

    sparkSession = SparkSession.builder()
      .appName("dataset_generator")
      .config(getInitializedSparkConf())
      .enableHiveSupport()
      .getOrCreate()

    startJob()
  }

  def startJob(): Unit = {
    // 解析配置文件
    parseFeatureConfigFile(featureConfigFilesPath)




  }

  def parseFeatureConfigFile(featureConfigFilePath: String): Unit = {
    var slotSet: Set[String] = null

  }
}
