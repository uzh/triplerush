package com.signalcollect.triplerush

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/**
 * special case p1 = p2:
 * 	predicate pair statistics are not computed
 * in all other cases:
 * 	predicate pair statistics are computed
 *
 * for a predicate-pair (p1, p2)
 * 	in-out is the number of distinct bindings for variable o in the store: (*, p1, o) (o, p2, *)
 * 	in-in is the number of distinct bindings for variable o in the store: (*, p1, o) (*, p2, o)
 * 	out-in is the number of distinct bindings for variable s in the store: (s, p1, *) (*, p2, s)
 * 	out-out is the number of distinct bindings for variable s in the store: (s, p1, *) (s, p2, *)
 *
 * for every predicate p, the branching statistics is:
 * 	the number of triples corresponding to this pattern of this form in the store: (*, p1, *)
 */

class PredicateSelectivity(tr: TripleRush) {
  val s = -1
  val p = -2
  val o = -3

  val x = -4
  val y = -5

  def bindingsToMap(bindings: Array[Int]): Map[Int, Int] = {
    (((-1 to -bindings.length by -1).zip(bindings))).toMap
  }

  def getBindingsFor(variable: Int, bindings: Traversable[Array[Int]]): Set[Int] = {
    val allBindings: List[Map[Int, Int]] = bindings.toList.map(bindingsToMap(_).map(entry => (entry._1, entry._2)))
    val listOfSetsOfKeysWithVar: List[Set[Int]] = allBindings.map {
      bindings: Map[Int, Int] =>
        bindings.filterKeys(_ == variable).values.toSet
    }
    listOfSetsOfKeysWithVar.foldLeft(Set[Int]())(_ union _)
  }

  val queryToGetAllPredicates = QuerySpecification(List(TriplePattern(s, p, o)))
  val allPredicateResult = tr.executeQuery(queryToGetAllPredicates.toParticle)
  val bindingsForPredicates = getBindingsFor(p, allPredicateResult)
  val ps = bindingsForPredicates.size
  println(s"Computing selectivities for $ps * $ps = ${ps * ps} predicate combinations ...")

  var outOut = Map[(Int, Int), Int]().withDefaultValue(0)
  var inOut = Map[(Int, Int), Int]().withDefaultValue(0)
  var inIn = Map[(Int, Int), Int]().withDefaultValue(0)
  def outIn(p1: Int, p2: Int) = inOut((p2, p1))

  val optimizer = Some(new GreedyCardinalityOptimizer)
  val queriesTotal = ps * (ps - 1) * 3
  var queriesSoFar = 0
  for (p1 <- bindingsForPredicates) {
    for (p2 <- bindingsForPredicates) {
      if (p1 != p2) {
        println(s"Stats gathering progress: $queriesSoFar/$queriesTotal ...")
        val outOutQuery = QuerySpecification(List(TriplePattern(s, p1, x), TriplePattern(s, p2, y)))
        println(outOutQuery)
        val (outOutResult, stats1) = tr.executeAdvancedQuery(outOutQuery.toParticle, optimizer)
        outOut += (p1, p2) -> getBindingsFor(s, Await.result(outOutResult, 7200.seconds)).size

        val inOutQuery = QuerySpecification(List(TriplePattern(x, p1, o), TriplePattern(o, p2, y)))
        println(inOutQuery)
        val (inOutResult, stats2) = tr.executeAdvancedQuery(inOutQuery.toParticle, optimizer)
        inOut += (p1, p2) -> getBindingsFor(o, Await.result(inOutResult, 7200.seconds)).size

        val inInQuery = QuerySpecification(List(TriplePattern(x, p1, o), TriplePattern(y, p2, o)))
        println(inInQuery)
        val (inInResult, stats3) = tr.executeAdvancedQuery(inInQuery.toParticle, optimizer)
        inIn += (p1, p2) -> getBindingsFor(o, Await.result(inInResult, 7200.seconds)).size

        queriesSoFar += 3
      }
    }

  }
}