package com.ipponusa

import com.holdenkarau.spark.testing.{RDDComparisons, RDDGenerator, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MainSampleTestingTest extends FlatSpec with SharedSparkContext with Matchers with Checkers {

  behavior of "counter"

  it should "count words as expected" in {
    val text =
      """Hello world
        |Hello
      """.stripMargin

    val inputRdd: RDD[String] = sc.parallelize(List(text))
    val expectedRdd: RDD[(String, Int)] = sc.parallelize(List(("Hello", 2), ("world", 1)))

    val resRdd: RDD[(String, Int)] = WordCounter.count(inputRdd)
    assert(None === RDDComparisons.compare(resRdd, expectedRdd))
  }

  it should "have stable count, with generated RDDs" in {
    val stableProperty =
      forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {
        rdd => None === RDDComparisons.compare(WordCounter.count(rdd), WordCounter.count(rdd))
      }

    check(stableProperty)
  }
}
