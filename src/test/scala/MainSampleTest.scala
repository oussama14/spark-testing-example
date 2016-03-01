import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import com.github.rbrugier.MainSample

class MainSampleTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sc:SparkContext = _

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-rdd")
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop()
  }

  behavior of "Count words"

  it should "count words in lines" in {
    val text =
      """Hello world
        |Hello
      """.stripMargin
    val lines: RDD[String] = sc.parallelize(List(text))
    val wordCounts: RDD[(String, Int)] = MainSample.count(lines)

    wordCounts.collect() should contain allOf (("Hello", 2), ("world", 1))
  }

}
