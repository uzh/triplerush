package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary

class PredicateSelectivitySpec extends FlatSpec with Checkers {

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
    assert(stats.outOut(p1, p2) == 1)
  }

  it should "correctly compute predicate selectivity for all two-pattern combinations" in {
    val patternCombos = for {
      sFirst <- List(s1, s2)
      oFirst <- List(s1, s2)
      sSecond <- List(s1, s2)
      oSecond <- List(s1, s2)
    } yield (TriplePattern(sFirst, p1, oFirst), TriplePattern(sSecond, p2, oSecond))

    for (combo <- patternCombos) {
      // Test if stats are correct for this combo.
      val tr = new TripleRush
      val (first, second) = combo
      tr.addEncodedTriple(first.s, first.p, first.o)
      tr.addEncodedTriple(second.s, second.p, second.o)
      tr.prepareExecution
      val stats = new PredicateSelectivity(tr)
      // Ensure counts are correct for this pattern combo.
      val outInResult = if (first.s == second.o) 1 else 0
      val inOutResult = if (first.o == second.s) 1 else 0
      val outOutResult = if (first.s == second.s) 1 else 0
      val inInResult = if (first.o == second.o) 1 else 0
      println(combo)
      assert(stats.outIn(first.p, second.p) == outInResult)
      assert(stats.inOut(first.p, second.p) == inOutResult)
      assert(stats.outOut(first.p, second.p) == outOutResult)
      assert(stats.inIn(first.p, second.p) == inInResult)
      tr.shutdown
    }
  }

  import TripleGenerators._
  implicit lazy val arbTriples = Arbitrary(genTriples map (_.toSet))

  it should "correctly compute predicate branching for every predicate" in {
    check((triples: Set[TriplePattern]) => {
      val tr = new TripleRush
      for (triple <- triples) {
        tr.addEncodedTriple(triple.s, triple.p, triple.o)
      }

      tr.prepareExecution
      val stats = new PredicateSelectivity(tr)
      val predicates = triples.map(_.p)
      for (predicate <- predicates) {
        val triplesWithPredicate = triples.filter(_.p == predicate).size
        assert(stats.triplesWithPredicate(predicate) == triplesWithPredicate)
      }
      tr.shutdown
      true
    }, minSuccessful(20))

  }
}
