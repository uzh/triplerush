package com.signalcollect.triplerush

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import com.signalcollect.triplerush.QueryParticle._

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
  val predicates = getBindingsFor(p, allPredicateResult)
   
  val ps = predicates.size
  println(s"Computing selectivities for $ps * $ps = ${ps * ps} predicate combinations ...")

  var outOut = Map[(Int, Int), Int]().withDefaultValue(0)
  var inOut = Map[(Int, Int), Int]().withDefaultValue(0)
  var inIn = Map[(Int, Int), Int]().withDefaultValue(0)
  def outIn(p1: Int, p2: Int) = inOut((p2, p1))

  val optimizer = Some(new GreedyCardinalityOptimizer)
  val queriesTotal = ps * (ps - 1) * 3
  val tickets = 1000000
  var queriesSoFar = 0
  for (p1 <- predicates) {
    for (p2 <- predicates) {
      if (p1 != p2) {
        println(s"Stats gathering progress: $queriesSoFar/$queriesTotal ...")
        val outOutQuery = QuerySpecification(List(TriplePattern(s, p1, x), TriplePattern(s, p2, y))).toParticle
        outOutQuery.writeTickets(tickets)
        val (outOutResult, outOutStats) = tr.executeAdvancedQuery(outOutQuery, optimizer)
        val inOutQuery = QuerySpecification(List(TriplePattern(x, p1, o), TriplePattern(o, p2, y))).toParticle
        inOutQuery.writeTickets(tickets)
        val (inOutResult, inOutStats) = tr.executeAdvancedQuery(inOutQuery, optimizer)
        val inInQuery = QuerySpecification(List(TriplePattern(x, p1, o), TriplePattern(y, p2, o))).toParticle
        inInQuery.writeTickets(tickets)
        val (inInResult, inInStats) = tr.executeAdvancedQuery(inInQuery, optimizer)

        val isCompleteOutOut = Await.result(outOutStats, 7200.seconds)("isComplete").asInstanceOf[Boolean]
        val isCompleteInOut = Await.result(inOutStats, 7200.seconds)("isComplete").asInstanceOf[Boolean]
        val isCompleteInIn = Await.result(inInStats, 7200.seconds)("isComplete").asInstanceOf[Boolean]

        val outOutResultSize = {
          val bindingsCount = getBindingsFor(s, Await.result(outOutResult, 7200.seconds)).size
          if (isCompleteOutOut) {
            bindingsCount
          } else {
            -1
            //math.max(bindingsCount, 1)
          }
        }
        val inOutResultSize = {
          val bindingsCount = getBindingsFor(o, Await.result(inOutResult, 7200.seconds)).size
          if (isCompleteInOut) {
            bindingsCount
          } else {
            -1
            //math.max(bindingsCount, 1)
          }
        }
        val inInResultSize = {
          val bindingsCount = getBindingsFor(o, Await.result(inInResult, 7200.seconds)).size
          if (isCompleteInIn) {
            bindingsCount
          } else {
            -1
            //math.max(bindingsCount, 1)
          }
        }

        outOut += (p1, p2) -> outOutResultSize
        inOut += (p1, p2) -> inOutResultSize
        inIn += (p1, p2) -> inInResultSize

        queriesSoFar += 3
      }
    }

  }
}