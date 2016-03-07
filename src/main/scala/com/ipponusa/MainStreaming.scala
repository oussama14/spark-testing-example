package com.ipponusa

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object MainStreaming {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-testing-example")

  val ssc = new StreamingContext(sparkConf, Seconds(5))

  def main(args: Array[String]) {

    val rddQueue = new mutable.Queue[RDD[Char]]()

    val input: InputDStream[Char] = ssc.queueStream(rddQueue, oneAtATime = false)
    StreamLogic.capitalize(input)
      .print()

    ssc.start()

    for (c <- 'a' to 'z') {
      println("c = " + c)
      rddQueue += ssc.sparkContext.parallelize(List(c))
      Thread.sleep(Seconds(1).milliseconds)
    }

    println("await termination")
    ssc.awaitTermination()
  }


}
