package application

import com.alibaba.fastjson.{JSON, JSONObject}
import configuration.CommandLineArgumentsConfiguration._
import org.apache.spark.{SparkConf, SparkContext}
import util.ApplicationArguments
import util.ApplicationArguments.commandLine

import scala.collection.mutable

object ReduceUserPreferDataByUserId {

//  private val LOG = LoggerFactory.getLogger("debugger")

  private var sparkConf: SparkConf = null
  private var sparkContext: SparkContext = null

  def main(args: Array[String]): Unit = {
    val commandLine = ApplicationArguments.parseArgsToCommandLine(args)
    if (!ApplicationArguments.hasAllRequiredOptions(commandLine, Set(INPUT_DATA_PATH, OUTPUT_DATA_PATH))) {
      sys.exit(1)
    }

    sparkConf = new SparkConf().setAppName("spark-demo-98587")
    sparkContext = new SparkContext(sparkConf)
    val inputDataPath = commandLine.getOptionValue(INPUT_DATA_PATH)
    val outputDataPath = commandLine.getOptionValue(OUTPUT_DATA_PATH)

    jobStart(inputDataPath, outputDataPath)
  }

  def jobStart(inputDataPath: String, outputDataPath: String): Unit = {
    // todo 添加检验以下jsonkey是否一致
    // todo cate edu exp welfare
    // 无法检测一致，需要手工检测的是salary, local
    // {"cate":"2782_43|5146_9|2169_6|2488_5|2334_1","edu":"1_61|2_7","exp":"1_62|5_4|4_3","local":"7413,6040,6001,5985,5791,5786,1196,1147,1143,1142,1139,1_24","salary":"4000_6000_12|3000_5000_9|3500_4500_6|4000_4500_4|5000_8000_4|面议_4|4000_8000_4|4000_5000_3|1000以下_1","welfare":"8_47|10_38|7_29|1_23|9_22|2_21|12_15|11_13|6_11|4_6|5_5|3_4"}
    val result = sparkContext.textFile(inputDataPath).mapPartitions(iterator => {
      iterator.map(line => {
        val lineArray = line.split("\001")
        (lineArray(0), JSON.parseObject(lineArray(1)))
      })
    }).reduceByKey { case (jsonObjectA, jsonObjectB) => {
      putSameFlag(jsonObjectA, jsonObjectB)
    }}.mapPartitions(iterator => {
      iterator.map{ case (key , json) => {
        json.put("key", key)
        json.toJSONString
      }}
    }).repartition(4).saveAsTextFile(outputDataPath)
  }

  def putSameFlag(jsonObjectA: JSONObject, jsonObjectB: JSONObject): JSONObject ={
    val merge = new JSONObject
    var flag = true
    val attrArray = Array("cate", "edu", "exp", "welfare")
    attrArray.foreach(attr => {
      if (flag) {
        val a = jsonObjectA.getString(attr)
        val b = jsonObjectB.getString(attr)
        if (a.equals(b)) {
          flag = true
        } else {
          val idCountMap = mutable.Map[Int, Int]()
          a.split("\\|").foreach(str => {
            val strArray = str.split("_")
            idCountMap.put(strArray(0).toInt, strArray(1).toInt)
          })
          b.split("\\|").foreach(str => {
            val strArray = str.split("_")
            val (id, count) = (strArray(0).toInt, strArray(1).toInt)
            if (idCountMap.contains(id) && idCountMap.get(id) == count) {
              idCountMap.remove(id)
            } else {
              flag = false
            }
          })
          if (idCountMap.isEmpty) {
            flag = true
          } else {
            flag = false
          }
        }
      }
    })
    merge.put("flag", flag)
    merge.put("first", jsonObjectA)
    merge.put("second", jsonObjectB)
    merge
  }
}
