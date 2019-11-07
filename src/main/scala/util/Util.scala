package util



object Util {
  def getFileNameFromPath(filePath: String): String = {
    if (filePath == null || filePath.isEmpty())
      throw new RuntimeException(s"No such file $filePath")
    filePath.split("/").last
  }

  def main(args:Array[String]): Unit = {
    print(getFileNameFromPath("/home/hdp_lbg_supin/xingjiliang/job/feature_engineering/feature_configuration_files/setid_value_map.json"))
  }
}
