package com.ipponusa

import com.holdenkarau.spark.testing.StreamingSuiteBase

class StreamingWithSparkTestingTest extends StreamingSuiteBase {

  test("capitalize") {
    val input = List(List('a'), List('b'), List('c'))
    val expected = List(List('A'), List('B'), List('C'))

    testOperation(input, StreamLogic.capitalize, expected, ordered = false)
  }

}
