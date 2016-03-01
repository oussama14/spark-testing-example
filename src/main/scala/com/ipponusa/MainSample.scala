package com.ipponusa

import org.apache.spark.rdd.RDD


object MainSample {

  //val sparkConf = new SparkConf()
  //  .setMaster("local[*]")
   // .setAppName("spark-test-example")
  //val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) {
    println("Hello")
    //val rdd = sc.textFile("src/main/resources/learningSparkIntroduction.txt")
    //count(rdd).foreach(println)
  }

  def count(lines: RDD[String]): RDD[(String, Int)] = {
    val wordsCount = lines.flatMap(l => l.split("\\W+"))
        .map(word => word.trim())
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    wordsCount
  }
}
