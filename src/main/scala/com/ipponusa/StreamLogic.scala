package com.ipponusa

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

object StreamLogic {

  def capitalize(input: DStream[Char]): DStream[Char] = {
    input.map(_.toUpper)
  }

  def capitalizeWindowed(input: DStream[Char]): DStream[Char] = {
    input.map(_.toUpper)
          .window(windowDuration = Seconds(3), slideDuration = Seconds(2))
  }
}
