package application

import com.google.gson.reflect.TypeToken
import configuration.CommandLineArgumentsConfiguration._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import util.ApplicationArguments.commandLine
import util.{ApplicationArguments, DateUtil, Util}
import com.google.gson.{Gson, JsonParser}

import scala.collection.{JavaConverters, mutable}
import scala.io.Source

/**
  * 生成FNN样本，处理连续特征的缺省值，离散特征值全部+1，统计特征不做处理，交叉特征重新map，交叉统计保留但未来不会使用
  */
object GenerateFnnSample {

  private val LOG = LoggerFactory.getLogger("test")

  private var sparkConf:SparkConf = null
  private var sparkContext:SparkContext = null

  val addCommandLineOptions = {
    ApplicationArguments.addOption("s", START_DATE, true, "start date")
    ApplicationArguments.addOption("e", END_DATE, true, "end date")
    ApplicationArguments.addOption("d", "drop-features", true, "droppedFeatureList")
    ApplicationArguments.addOption("t", "type", true, "type")
    ApplicationArguments.addOption("c", "config", true, "configuration file name")
  }

  def main(args: Array[String]): Unit = {
    val commandLine = ApplicationArguments.parseArgsToCommandLine(args)
    if (!ApplicationArguments.hasAllRequiredOptions(commandLine, Set(INPUT_DATA_PATH, OUTPUT_DATA_PATH, START_DATE, END_DATE, "type"))) {
      sys.exit(1)
    }
    sparkConf = new SparkConf().setAppName("GenerateFnnSample")
    sparkContext = new SparkContext(sparkConf)
    jobStart()
  }

  def jobStart(): Unit = {
    val jsonParser = new JsonParser()
    val element = jsonParser.parse(
      Source.fromFile(Util.getFileNameFromPath(commandLine.getOptionValue("config"))).getLines.mkString
    )

    if (element.isJsonObject) {
      val jsonObject = element.getAsJsonObject
      val gson = new Gson()
      val discreteFeatureList = JavaConverters.asScalaBufferConverter[String](gson.fromJson(jsonObject.get("discrete"),
        new TypeToken[java.util.List[String]](){}.getType)).asScala.toList
      val continuousFeatureList = JavaConverters.asScalaBufferConverter[String](gson.fromJson(jsonObject.get("continuous"),
        new TypeToken[java.util.List[String]](){}.getType)).asScala.toList
      val orderedFeatureList = JavaConverters.asScalaBufferConverter[String](gson.fromJson(jsonObject.get("ordered"),
        new TypeToken[java.util.List[String]](){}.getType)).asScala.toList
      val crossFeatureSetId_valueIndexMap_map = JavaConverters.mapAsScalaMapConverter[String, java.util.Map[String, String]](
        gson.fromJson(jsonObject.get("cross"), new TypeToken[java.util.Map[String, java.util.Map[String, String]]](){}.getType))
        .asScala.mapValues((value:java.util.Map[String, String]) => {JavaConverters.mapAsScalaMapConverter(value).asScala.toMap}).map(x=>x)


      val zeroStartList = List("1063", "1064", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "21")
//      val oneStartList = List("1066")
      // 3010 if == -1 to 0
      // 20 if > 12 drop if < 2 drop

      val slotMap = Map("1" -> "0", "2" -> "1" , "9" -> "2")
      val i_wyMap = Map("-1" -> "0", "1" -> "1" , "4" -> "2", "5" -> "3", "6" -> "4", "7" -> "5", "8" ->"6", "9" -> "7")
      val cityMap = getCityOrCateMap("city_map")
      val cate2Map = getCityOrCateMap("cate2_map")
      val cate3Map = getCityOrCateMap("cate3_map")

      val discreteFeatureListBroadCastValue = sparkContext.broadcast(discreteFeatureList)
      val continuousFeatureListBroadCastValue = sparkContext.broadcast(continuousFeatureList)
      val orderedFeatureListBroadCastValue = sparkContext.broadcast(orderedFeatureList)
      val crossFeatureMapBroadCastValue = sparkContext.broadcast(crossFeatureSetId_valueIndexMap_map)

      val slotMapBroadCastValue = sparkContext.broadcast(slotMap)
      val i_wyMapBroadCastValue = sparkContext.broadcast(i_wyMap)
      val cityMapBroadCastValue = sparkContext.broadcast(cityMap)
      val cate2MapBroadCastValue = sparkContext.broadcast(cate2Map)
      val cate3MapBroadCastValue = sparkContext.broadcast(cate3Map)

      DateUtil.getDateIntervalDays(commandLine.getOptionValue(START_DATE), commandLine.getOptionValue(END_DATE))
        .foreach(dateString => {
          sparkContext.textFile(commandLine.getOptionValue(INPUT_DATA_PATH) + "/" + dateString + "/" + commandLine.getOptionValue("type")).repartition(512).mapPartitions(iterator => {
            val discreteFeatureList = discreteFeatureListBroadCastValue.value
            val continuousFeatureList = continuousFeatureListBroadCastValue.value
            val orderedFeatureList = orderedFeatureListBroadCastValue.value
            val crossFeatureSetId_valueIndexMap_map = crossFeatureMapBroadCastValue.value

            val slotMap = slotMapBroadCastValue.value
            val i_wyMap = i_wyMapBroadCastValue.value
            val cityMap = cityMapBroadCastValue.value
            val cate2Map = cate2MapBroadCastValue.value
            val cate3Map = cate3MapBroadCastValue.value

            iterator.map(line => {
              var flag = true
              val splitLineList = line.split("\t")
              val label = splitLineList(1)
              var valueIsDefaultList = List[String]()
              val cleanedFeatureIdValueMapArray = splitLineList.drop(2).map(featureIdValueMap => {
                val setIdAndValue = featureIdValueMap.split(":")
                var (setId, value) = (setIdAndValue(0).trim, setIdAndValue(1).trim)
                if (discreteFeatureList.contains(setId)) {
                  if (zeroStartList.contains(setId)) {
                    if (value.toInt < 0) {
                      flag = false
                    }
                  } else if ("17".equals(setId)) {
                    value = i_wyMap.getOrElse(value, "0")
                  } else if ("1065".equals(setId)) {
                    value = slotMap.getOrElse(value, "3")
                  } else if ("3010".equals(setId) && value.equals("-1")) {
                    value = "0"
                  } else if ("1066".equals(setId)){
                    value = (value.toInt - 1).toString
                  } else if ("1060".equals(setId) || "28".equals(setId)) {
                    println(s"setid:1060,value:${value},idx:${cityMap.getOrElse(value, "0")}")
                    value = cityMap.getOrElse(value, "0")
                  } else if ("32".equals(setId)) {
                    // 二级地域
                    println(s"setid:32,value:${value},idx:${cate2Map.getOrElse(value, "0")}")
                    value = cate2Map.getOrElse(value, "0")
                  } else if ("33".equals(setId)) {
                    // 三级地域
                    println(s"setid:33,value:${value},idx:${cate3Map.getOrElse(value, "0")}")
                    value = cate3Map.getOrElse(value, "0")
                  } else {
                    value = (value.toInt + 1).toString
                  }
                } else if (continuousFeatureList.contains(setId)) {
                  if ("-1.0".equals(value)) {
                    value = "0.0"
                    valueIsDefaultList = valueIsDefaultList :+ "1"
                  } else {
                    valueIsDefaultList = valueIsDefaultList :+ "0"
                  }
                } else if (orderedFeatureList.contains(setId)) {
                  if ("20".equals(setId)) {
                    if (value.toInt > 12) {
                      value = "12"
                    }
                  }
                } else if (crossFeatureSetId_valueIndexMap_map.contains(setId)) {
                  try {
                    value = crossFeatureSetId_valueIndexMap_map.get(setId).get(value)
                  } catch {
                    case e: Throwable => {
                      println(s"获取不到value!${setId}-${value}")
                      value = "0"
                    }
                  }
                } else {
                }
                value
              })
              if (flag) {
                cleanedFeatureIdValueMapArray.mkString("\t") + "\t" + valueIsDefaultList.mkString("\t") + "\t" + label
              } else {
                None
              }
            }).filter(line => {line != None})
          }).repartition(1).saveAsTextFile(commandLine.getOptionValue(OUTPUT_DATA_PATH) + "/" + dateString + "/" + commandLine.getOptionValue("type"))
        })
    }
  }

  def getCityOrCateMap(filePath: String):Map[String, String] = {
    Source.fromFile(Util.getFileNameFromPath(filePath)).getLines.map(line => {
      val splitedLineList = line.split("\t")
      (splitedLineList(0), splitedLineList(1))
    }).toMap
  }

}