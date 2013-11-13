//package com.signalcollect.triplerush
//
//import scala.concurrent.Await
//import scala.concurrent.duration.DurationInt
//import org.scalatest.FlatSpec
//import org.scalatest.prop.Checkers
//import org.specs2.matcher.ShouldMatchers
//import com.signalcollect.triplerush.vertices.QueryOptimizer
//import com.signalcollect.triplerush.ArbUtil._
//import com.signalcollect.triplerush.QueryParticle._
//import scala.util.Random
//import scala.annotation.tailrec
//
//class IntegrationSpec extends FlatSpec with ShouldMatchers with Checkers {
//
//  def qe1 = {
//    val qe = new QueryEngine
//    qe.addEncodedTriple(1, 2, 3)
//    qe.addEncodedTriple(3, 5, 6)
//    qe.addEncodedTriple(1, 2, 100)
//    qe.addEncodedTriple(101, 5, 6)
//    qe.addEncodedTriple(1, 2, 4)
//    qe.addEncodedTriple(4, 5, 6)
//    qe.awaitIdle
//    qe
//  }
//
//  @tailrec
//  final def randomPositiveInt: Int = {
//    val base = Random.nextInt
//    if (base != Int.MinValue) {
//      math.abs(base)
//    } else {
//      // Abs does not work as expected on Int.MinValue, so we have to treat
//      // that case separately by just doing it all over.
//      randomPositiveInt
//    }
//  }
//
//  def isRoot(tp: TriplePattern): Boolean = {
//    tp == TriplePattern(0, 0, 0)
//  }
//
//  val root = TriplePattern(0, 0, 0)
//
//  def replaceNegativeAndWildcardWithVariable(tp: TriplePattern): TriplePattern = {
//    TriplePattern(
//      if (tp.s > 0) tp.s else -1,
//      if (tp.p > 0) tp.p else -1,
//      if (tp.o > 0) tp.o else -1)
//  }
//
//  def replaceNegativeAndWildcardWithRandomConst(tps: TriplePattern*): List[TriplePattern] = {
//    val randomPositive = randomPositiveInt
//    tps.map { tp =>
//      TriplePattern(
//        if (tp.s > 0) tp.s else randomPositive,
//        if (tp.p > 0) tp.p else randomPositive,
//        if (tp.o > 0) tp.o else randomPositive)
//    }.toList
//  }
//
//  "TripleRush" should "support matching two triples" in {
//    val qe = new QueryEngine
//    qe.addEncodedTriple(1, 2, 3)
//    qe.addEncodedTriple(4, 5, 3)
//    qe.awaitIdle
//    val q = QuerySpecification(Array(TriplePattern(-1, -2, 3)))
//    val result = Await.result(qe.executeQuery(q.toParticle), 2 seconds)
//    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
//    qe.shutdown
//    assert(bindings === Set(List(1, 2), List(4, 5)))
//  }
//
//  it should "support matching one thousand triples" in {
//    val qe = new QueryEngine
//    for (i <- 1 to 1000) {
//      qe.addEncodedTriple(i, i + 1, (i % 3) + 1)
//    }
//    qe.awaitIdle
//    val expected = (1 to 1000).filter(i => (i % 3) + 1 == 3).map(i => List(i, i + 1)).toSet
//    val q = QuerySpecification(Array(TriplePattern(-1, -2, 3)))
//    val result = Await.result(qe.executeQuery(q.toParticle), 2 seconds)
//    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
//    qe.shutdown
//    assert(bindings === expected)
//  }
//
//  it should "support binary search in S*O index vertex" in {
//    val qe = qe1
//    val q = QuerySpecification(Array(
//      TriplePattern(1, 2, -1),
//      TriplePattern(-1, 5, 6)))
//    val result = Await.result(qe.executeQuery(q.toParticle), 2 seconds)
//    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
//    qe.shutdown
//    assert(bindings === Set(List(3), List(4)))
//  }
//
//  it should "support redundant patterns" in {
//    val qe = qe1
//    val q = QuerySpecification(Array(
//      TriplePattern(1, 2, -1),
//      TriplePattern(-1, 5, 6),
//      TriplePattern(-1, 5, 6)))
//    val result = Await.result(qe.executeQuery(q.toParticle), 2 seconds)
//    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
//    qe.shutdown
//    assert(bindings === Set(List(3), List(4)))
//  }
//
//  it should "support queries with no results" in {
//    val qe = qe1
//    val q = QuerySpecification(Array(
//      TriplePattern(-2, 2, -1),
//      TriplePattern(-1, 5, 6),
//      TriplePattern(-2, 5, 6)))
//    val result = Await.result(qe.executeQuery(q.toParticle), 2 seconds)
//    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
//    qe.shutdown
//    assert(bindings === Set())
//  }
//
//  it should "deliver correct results for almost arbitrary queries on almost arbitrary triples" in {
//    check(
//      (triples: Array[TriplePattern], p1: TriplePattern, p2: TriplePattern) => {
//        println("Starting Q6")
//        val query = Array(replaceNegativeAndWildcardWithVariable(p1),
//          replaceNegativeAndWildcardWithVariable(p2)).
//          filter(_ != TriplePattern(-1, -1, -1)).distinct
//        val triplesInStore = replaceNegativeAndWildcardWithRandomConst(p1, p2) ++
//          replaceNegativeAndWildcardWithRandomConst(triples: _*)
//        println("In store: " + triplesInStore)
//        println("Query:    " + query.toList)
//        val qe = new QueryEngine
//        for (tp <- triplesInStore) {
//          qe.addEncodedTriple(tp.s, tp.p, tp.o)
//        }
//        qe.awaitIdle
//        val qp = QuerySpecification(query).toParticle
//        val result = Await.result(qe.executeQuery(qp), 2 seconds)
//        val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
//        qe.shutdown
//        println("Bindings: " + bindings.mkString(", "))
//        bindings.size >= 1 || (bindings.size == 0 && query.length == 0)
//      },
//      minSuccessful(1000))
//  }
//
//}
