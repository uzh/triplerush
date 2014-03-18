package com.signalcollect.triplerush.optimizers

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TestAnnouncements

class PredicateSelectivitySpec extends FlatSpec with Checkers with TestAnnouncements {

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

  "PredicateSelectivity" should "correctly compute predicate selectivity in a large example" in {
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
      val stats = new PredicateSelectivity(tr)
      assert(stats.outIn(p4, p2) == 3)
      assert(stats.outIn(p5, p2) == 2)
      assert(stats.outOut(p4, p5) == 2)
    } finally {
      tr.shutdown
    }
  }

  "PredicateSelectivity" should "correctly find the selectivity statistics" in {
    val tr = new TripleRush
    try {
      tr.addEncodedTriple(s1, p1, o1)
      tr.addEncodedTriple(s1, p1, o2)
      tr.addEncodedTriple(s1, p1, o3)
      tr.addEncodedTriple(s1, p1, o4)
      tr.addEncodedTriple(s1, p1, o5)
      tr.addEncodedTriple(s1, p1, o6)
      tr.addEncodedTriple(s1, p1, o7)
      tr.addEncodedTriple(s1, p1, o8)
      tr.addEncodedTriple(s1, p1, o9)

      tr.addEncodedTriple(s1, p2, o5)
      tr.addEncodedTriple(s1, p2, o6)
      tr.addEncodedTriple(s1, p2, o7)
      tr.addEncodedTriple(s1, p2, o8)
      tr.addEncodedTriple(s1, p2, o9)

      tr.addEncodedTriple(s2, p1, o3)
      tr.addEncodedTriple(s2, p1, o4)
      tr.addEncodedTriple(s2, p1, o5)
      tr.addEncodedTriple(s2, p1, o6)

      tr.addEncodedTriple(s2, p2, o2)
      tr.addEncodedTriple(s2, p2, o3)
      tr.addEncodedTriple(s2, p2, o4)
      tr.addEncodedTriple(s2, p2, o5)
      tr.addEncodedTriple(s2, p2, o6)
      tr.addEncodedTriple(s2, p2, o7)

      tr.addEncodedTriple(s2, p3, o2)
      tr.addEncodedTriple(s2, p4, o3)
      tr.addEncodedTriple(s2, p5, o4)
      tr.addEncodedTriple(s1, p4, o6)
      tr.addEncodedTriple(s1, p4, o7)
      tr.addEncodedTriple(s1, p4, o8)
      tr.addEncodedTriple(s3, p3, o5)
      tr.addEncodedTriple(s3, p2, o10)
      tr.addEncodedTriple(s2, p3, o5)

      tr.addEncodedTriple(o5, p4, o1)
      tr.addEncodedTriple(o5, p4, o2)
      tr.addEncodedTriple(o5, p4, o3)
      tr.addEncodedTriple(o4, p4, o7)
      tr.addEncodedTriple(o4, p4, o9)
      tr.addEncodedTriple(o3, p4, o8)
      tr.addEncodedTriple(o3, p4, o9)
      tr.addEncodedTriple(o3, p4, o10)
      tr.addEncodedTriple(o2, p4, o7)

      tr.addEncodedTriple(o3, p3, o1)
      tr.addEncodedTriple(o4, p3, o1)
      tr.addEncodedTriple(o5, p3, o2)
      tr.addEncodedTriple(o9, p3, o4)
      tr.addEncodedTriple(o10, p3, o3)
      tr.addEncodedTriple(o11, p3, o4)

      tr.addEncodedTriple(o3, p5, o9)
      tr.addEncodedTriple(o10, p5, o9)

      tr.prepareExecution

      val stats = new PredicateSelectivity(tr)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly compute predicate selectivity for all two-pattern combinations" in {
    val patternCombos = for {
      sFirst <- List(s1, s2)
      oFirst <- List(s1, s2)
      sSecond <- List(s1, s2)
      oSecond <- List(s1, s2)
    } yield (TriplePattern(sFirst, p1, oFirst), TriplePattern(sSecond, p2, oSecond))

    val tr = new TripleRush(optimizerCreator = NoOptimizerCreator)
    try {
      for (combo <- patternCombos) {
        // Test if stats are correct for this combo.
        val (first, second) = combo
        tr.addEncodedTriple(first.s, first.p, first.o)
        tr.addEncodedTriple(second.s, second.p, second.o)
      }
      tr.prepareExecution
      val stats = new PredicateSelectivity(tr)
      for (combo <- patternCombos) {
        val (first, second) = combo
        // Ensure counts are correct for this pattern combo.
        val outInResult = 8
        val inOutResult = 8
        val outOutResult = 8
        val inInResult = 8
        assert(stats.outIn(first.p, second.p) == outInResult, s"Problematic outIn stat for $combo: Is ${stats.outIn(first.p, second.p)}, should be $outInResult.")
        assert(stats.inOut(first.p, second.p) == inOutResult, s"Problematic inOut stat for $combo: Is ${stats.inOut(first.p, second.p)}, should be $inOutResult.")
        assert(stats.outOut(first.p, second.p) == outOutResult, s"Problematic outOut stat for $combo: Is ${stats.outOut(first.p, second.p)}, should be $outOutResult.")
        assert(stats.inIn(first.p, second.p) == inInResult, s"Problematic inIn stat for $combo: Is ${stats.inIn(first.p, second.p)}, should be $inInResult.")
      }
    } finally {
      tr.shutdown
    }
  }
}
