package application

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object LibsvmSampleToTfrecords {

  val LOG = LoggerFactory.getLogger("debugger")
  var sparkContext: SparkContext = null

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("make_fnn_sample").setMaster("local[*]")
    sparkContext = new SparkContext(sparkConf)

  }
}
