package com.ipponusa

import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-testing-example")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) {
    val countByWordRdd = sc.textFile("src/main/resources/intro.txt")
      .flatMap(l => l.split("\\W+"))
      .map(word => word.trim())
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    countByWordRdd
      .foreach(println)
  }
}

//  def count(lines: RDD[String]): RDD[(String, Int)] = {
//    val wordsCount = lines.flatMap(l => l.split("\\W+"))
//        .map(word => word.trim())
//      .map(word => (word, 1))
//      .reduceByKey(_ + _)
//    wordsCount
//  }
