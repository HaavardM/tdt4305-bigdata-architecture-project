
import org.apache.spark.{SparkConf, SparkContext}

object task1 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("task1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val helloWorldString = "Hello world"
    println(helloWorldString)
    sc.stop()
  }
}