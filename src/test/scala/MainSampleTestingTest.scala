import com.github.rbrugier.MainSample
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, RDDGenerator, DatasetSuiteBase}
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

class MainSampleTestingTest extends FlatSpec with SharedSparkContext with Matchers {

  behavior of "counter"

  it should "count words expected" in {
    val text =
      """Hello world
        |Hello
      """.stripMargin

    val inputRdd = sc.parallelize(List(text))
    val expectedRdd: RDD[(String, Int)] = sc.parallelize(List(("Hello", 2), ("world", 1)))

    val resRdd: RDD[(String, Int)] = MainSample.count(inputRdd)
    assert(None === RDDComparisons.compare(resRdd, expectedRdd))
  }
}
