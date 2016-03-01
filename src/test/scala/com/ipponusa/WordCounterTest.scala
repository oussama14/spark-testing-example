package com.ipponusa

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class WordCounterTest extends FlatSpec with Matchers with BeforeAndAfter {

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
    val wordCounts: RDD[(String, Int)] = WordCounter.count(lines)

    wordCounts.collect() should contain allOf (("Hello", 2), ("world", 1))
  }

}
