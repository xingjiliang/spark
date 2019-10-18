package util

import configuration.CommandLineArgumentsConfiguration._
import org.apache.commons.cli.{CommandLine, Option, OptionBuilder, Options, PosixParser}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

class ApplicationArguments {

}

object ApplicationArguments {
  private val LOG = LoggerFactory.getLogger("debugger")

  val options = new Options
  var commandLine: CommandLine = null

  val addDefaultOptions: Unit = {
    options.addOption("i", INPUT_DATA_PATH, true, "input data path")
    options.addOption("o", OUTPUT_DATA_PATH,true, "output data path")
  }

  def addOption(short: String, long: String, hasArg: Boolean, desc: String): Unit = {
    options.addOption(new Option(short, long, hasArg, desc))
  }

  def parseArgs(args: Array[String]): Unit = {
    try {
      commandLine = new PosixParser().parse(options, args, false)
    } catch {
      case e: Exception => {println("解析参数出错，输入的参数是:\n"+args);help();}
    }
  }

  def help(): Unit = {
    println()
    val stringBuilder: StringBuilder = new StringBuilder().append("Options: \n")
    println(stringBuilder)
    sys.exit(1)
  }

  def hasAllRequiredOptions(requiredOptions: List[String]): Boolean = {
    requiredOptions.foreach(requiredOption => {
      if (!commandLine.hasOption(requiredOption)) {
        return false
      }
    })
    return true
  }

  def main(args: Array[String]): Unit = {
    val argv = Array[String]("-Dkey1=value1", "-Dkey2=value2")
    parseArgs(argv)
    JavaConverters.asScalaSetConverter(commandLine.getOptionProperties("D").stringPropertyNames()).asScala.foreach(d => {
      println(d)
    })
    println()
  }
}