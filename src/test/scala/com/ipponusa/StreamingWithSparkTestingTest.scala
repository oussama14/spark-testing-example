package com.ipponusa

import com.holdenkarau.spark.testing.StreamingSuiteBase

class StreamingWithSparkTestingTest extends StreamingSuiteBase {

  test("capitalize") {
    val input = List(List('a'), List('b'), List('c'))
    val expected = List(List('A'), List('B'), List('C'))

    testOperation(input, StreamLogic.capitalize, expected, ordered = false)
  }

  test("capitalize by window") {
    val input = List(List('a'), List('b'), List('c'), List('d'), List('e'))
    val expected = List(List('A','B'), List('B','C','D'))

    testOperation(input, StreamLogic.capitalizeWindowed, expected, ordered = false)
  }

}
