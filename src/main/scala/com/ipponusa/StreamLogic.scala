package com.ipponusa

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

object StreamLogic {

  def applyOperations(input: InputDStream[Char]): DStream[Char] = {
    input.map(_.toUpper)
      .window(windowDuration = Seconds(3), slideDuration = Seconds(2))
  }
}
