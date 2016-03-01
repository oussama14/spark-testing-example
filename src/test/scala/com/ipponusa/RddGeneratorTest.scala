package com.ipponusa

import com.holdenkarau.spark.testing.{RDDGenerator, SharedSparkContext}
import org.junit.runner.RunWith
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

@RunWith(classOf[JUnitRunner])
class RddGeneratorTest extends FunSuite with SharedSparkContext with Checkers {
  // tag::propertySample[]
  // A trivial property that the map doesn't change the number of elements
  test("map should not change number of elements") {
    val property =
      forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {
        rdd => rdd.map(_.length).count() == rdd.count()
      }

    check(property)
  }
}


