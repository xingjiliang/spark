import org.apache.spark.{SparkConf, SparkContext}

object ScalaSparkTest {
  var sparkConf: SparkConf = null
  var sparkContext: SparkContext = null

  def main(args: Array[String]) = {
    sparkConf = new SparkConf().setAppName("spark-demo-98587").setMaster("local[*]")
    sparkContext = new SparkContext(sparkConf)
    sparkContext.textFile("D:\\test.txt").foreach(println)
  }
}
