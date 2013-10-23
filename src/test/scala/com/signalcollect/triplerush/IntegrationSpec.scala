package com.signalcollect.triplerush

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.specs2.matcher.ShouldMatchers

class IntegrationSpec extends FlatSpec with ShouldMatchers {

  "TripleRush" should "support matching two triples" in {
    val qe = new QueryEngine
    qe.addEncodedTriple(1, 2, 3)
    qe.addEncodedTriple(4, 5, 3)
    val q = QuerySpecification(Array(TriplePattern(-1, -2, 3)), 1)
    val result = Await.result(qe.executeQuery(q), 10 seconds)
    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
    qe.shutdown
    assert(bindings === Set(List(1, 2), List(4, 5)))
  }

  it should "support matching one thousand triples" in {
    val qe = new QueryEngine
    for (i <- 1 to 1000) {
      qe.addEncodedTriple(i, i + 1, (i % 3) + 1)
    }
    val expected = (1 to 1000).filter(i => (i % 3) + 1 == 3).map(i => List(i, i + 1)).toSet
    val q = QuerySpecification(Array(TriplePattern(-1, -2, 3)), 2)
    val result = Await.result(qe.executeQuery(q), 10 seconds)
    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
    qe.shutdown
    assert(bindings === expected)
  }

  it should "support binary search in S*O index vertex" in {
    val qe = new QueryEngine
    qe.addEncodedTriple(1, 2, 3)
    qe.addEncodedTriple(3, 5, 6)
    qe.addEncodedTriple(1, 2, 100)
    qe.addEncodedTriple(101, 5, 6)
    qe.addEncodedTriple(1, 2, 4)
    qe.addEncodedTriple(4, 5, 6)
    val q = QuerySpecification(Array(
      TriplePattern(1, 2, -1),
      TriplePattern(-1, 5, 6)), 3)
    val result = Await.result(qe.executeQuery(q), 10 seconds)
    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
    qe.shutdown
    assert(bindings === Set(List(3), List(4)))
  }

  it should "support redundant patterns" in {
    val qe = new QueryEngine
    qe.addEncodedTriple(1, 2, 3)
    qe.addEncodedTriple(3, 5, 6)
    qe.addEncodedTriple(1, 2, 100)
    qe.addEncodedTriple(101, 5, 6)
    qe.addEncodedTriple(1, 2, 4)
    qe.addEncodedTriple(4, 5, 6)
    val q = QuerySpecification(Array(
      TriplePattern(1, 2, -1),
      TriplePattern(-1, 5, 6),
      TriplePattern(-1, 5, 6)), 4)
    val result = Await.result(qe.executeQuery(q), 10 seconds)
    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
    qe.shutdown
    assert(bindings === Set(List(3), List(4)))
  }

}
