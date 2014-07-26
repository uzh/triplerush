package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary
import com.signalcollect.triplerush.optimizers.CleverCardinalityOptimizer

class IgnoredBindingsSpec extends FlatSpec with Checkers with TestAnnouncements {

  val s1 = 1
  val s2 = 2
  val s3 = 3
  val o1 = 101
  val o2 = 102
  val o3 = 103
  val o4 = 104
  val o5 = 105
  val o6 = 106
  val o7 = 107
  val o8 = 108
  val o9 = 109
  val o10 = 110
  val o11 = 111
  val p1 = 1001
  val p2 = 1002
  val p3 = 1003
  val p4 = 1004
  val p5 = 1005

  "ChildIdsForPattern" should "correctly return all the predicates from the root vertex" in {
    val tr = new TripleRush
    try {
      tr.addEncodedTriple(s1, p1, o1)
      tr.addEncodedTriple(s2, p1, o2)
      tr.addEncodedTriple(s1, p2, o3)
      tr.addEncodedTriple(s1, p2, o4)
      tr.addEncodedTriple(s3, p2, o10)
      tr.addEncodedTriple(s2, p3, o5)
      tr.addEncodedTriple(o5, p4, o6)
      tr.addEncodedTriple(o4, p4, o7)
      tr.addEncodedTriple(o3, p4, o8)
      tr.addEncodedTriple(o10, p4, o11)
      tr.addEncodedTriple(o3, p5, o9)
      tr.addEncodedTriple(o10, p5, o9)
      tr.prepareExecution
      val predicates = tr.childIdsForPattern(EfficientIndexPattern(0, 0, 0)).toSet
      assert(predicates === Set(p1, p2, p3, p4, p5))
    } finally {
      tr.shutdown
    }
  }

  "An index query" should "be able to retrieve all predicates" in {
    val tr = new TripleRush
    try {
      tr.addEncodedTriple(s1, p1, o1)
      tr.addEncodedTriple(s2, p1, o2)
      tr.addEncodedTriple(s1, p2, o3)
      tr.addEncodedTriple(s1, p2, o4)
      tr.addEncodedTriple(s3, p2, o10)
      tr.addEncodedTriple(s2, p3, o5)
      tr.addEncodedTriple(o5, p4, o6)
      tr.addEncodedTriple(o4, p4, o7)
      tr.addEncodedTriple(o3, p4, o8)
      tr.addEncodedTriple(o10, p4, o11)
      tr.addEncodedTriple(o3, p5, o9)
      tr.addEncodedTriple(o10, p5, o9)
      tr.prepareExecution
      val predicates = tr.childIdsForPattern(EfficientIndexPattern(0, 0, 0))
      assert(predicates.toSet === Set(p1, p2, p3, p4, p5))
    } finally {
      tr.shutdown
    }
  }

  def getBindingsFor(variable: Int, bindings: Traversable[Array[Int]]): Set[Int] = {
    val allBindings: List[Map[Int, Int]] = bindings.toList.map(bindingsToMap(_).map(entry => (entry._1, entry._2)))
    val listOfSetsOfKeysWithVar: List[Set[Int]] = allBindings.map {
      bindings: Map[Int, Int] =>
        bindings.filterKeys(_ == variable).values.toSet
    }
    listOfSetsOfKeysWithVar.foldLeft(Set[Int]())(_ union _)
  }

  def bindingsToMap(bindings: Array[Int]): Map[Int, Int] = {
    (((-1 to -bindings.length by -1).zip(bindings))).toMap
  }

}
