package configuration

object PathConfiguration {
  val ROOT_PATH: String = System.getProperty("user.dir")

  val RESOURCES_PATH: String = ROOT_PATH + "/src/main/resources"

  val CLASS_LOADER_PATH: String = PathConfiguration.getClass.getClassLoader.getResource("").getPath

  def main(args:Array[String]) : Unit =  {
    println(CLASS_LOADER_PATH)
  }
}
