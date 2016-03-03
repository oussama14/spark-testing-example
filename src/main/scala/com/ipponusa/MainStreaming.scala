package com.ipponusa

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object MainStreaming {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-testing-example")

  val ssc = new StreamingContext(sparkConf, Seconds(5))

  def main(args: Array[String]) {

    val rddQueue = new mutable.Queue[RDD[Int]]()

    ssc.queueStream(rddQueue, oneAtATime=false)
      .filter(i => i % 2 ==0)
      .map(i => i * i)
      .print()

    ssc.start()

    for (i <- 1 to 30) {
      rddQueue += ssc.sparkContext.parallelize(List(i))
      Thread.sleep(Seconds(1).milliseconds)
    }

    println("await termination")
    ssc.awaitTermination()
  }
}
