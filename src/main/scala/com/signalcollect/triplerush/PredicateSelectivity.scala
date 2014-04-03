package com.signalcollect.triplerush

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import com.signalcollect.triplerush.optimizers.GreedyCardinalityOptimizer

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

  /*def estimateBranchingFactor(explored: TriplePattern, next: TriplePattern): Long = {
    if (explored.p > 0 && next.p > 0) {
      next match {
        case TriplePattern(explored.s, explored.p, explored.o) =>
          1
        case TriplePattern(explored.s, p, explored.o) =>
          math.min(outOut(explored.p, p), inIn(explored.p, p))
        case TriplePattern(_, p, explored.o) =>
          inIn(explored.p, p)
        case TriplePattern(explored.s, p, _) =>
          outOut(explored.p, p)
        case TriplePattern(explored.o, p, explored.s) =>
          math.min(inOut(explored.p, p), outIn(explored.p, p))
        case TriplePattern(_, p, explored.s) =>
          outIn(explored.p, p)
        case TriplePattern(explored.o, p, _) =>
          inOut(explored.p, p)
        case other =>
          Long.MaxValue
      }
    } else {
      Long.MaxValue
    }
  }*/

  val predicates = tr.childIdsForPattern(TriplePattern(0, 0, 0))

  val ps = predicates.size
  //println(s"Computing selectivities for $ps * $ps = ${ps * ps} predicate combinations ...")

  var outOut = Map[(Int, Int), Long]()
  var inOut = Map[(Int, Int), Long]()
  var inIn = Map[(Int, Int), Long]()
  def outIn(p1: Int, p2: Int) = inOut((p2, p1))

  val optimizer = Some(GreedyCardinalityOptimizer)
  val queriesTotal = ps * ps * 3
  val tickets = Long.MaxValue
  var queriesSoFar = 0
  //println(s"Gathering index statistics ...")
  var lastPrintedProgressPercentage = 0.0
  for (p1 <- predicates) {
    for (p2 <- predicates) {
      val currentProgressPercentage = queriesSoFar / queriesTotal.toDouble
      /*if (currentProgressPercentage - lastPrintedProgressPercentage >= 0.1) {
        println(s"selectivity progress: ${(currentProgressPercentage * 100).toInt}%")
        lastPrintedProgressPercentage = currentProgressPercentage
      }*/
      val outOutQuery = QuerySpecification(List(TriplePattern(s, p1, x), TriplePattern(s, p2, y)), tickets)
      val outOutResult = tr.executeCountingQuery(outOutQuery, optimizer)
      val inOutQuery = QuerySpecification(List(TriplePattern(x, p1, o), TriplePattern(o, p2, y)), tickets)
      val inOutResult = tr.executeCountingQuery(inOutQuery, optimizer)
      val inInQuery = QuerySpecification(List(TriplePattern(x, p1, o), TriplePattern(y, p2, o)), tickets)
      val inInResult = tr.executeCountingQuery(inInQuery, optimizer)

      // TODO: Handle the else parts better.
      val outOutResultSize = Await.result(outOutResult, 600.seconds).get
      val inOutResultSize = Await.result(inOutResult, 600.seconds).get
      val inInResultSize = Await.result(inInResult, 600.seconds).get

      outOut += (p1, p2) -> outOutResultSize
      inOut += (p1, p2) -> inOutResultSize
      inIn += (p1, p2) -> inInResultSize

      queriesSoFar += 3
    }
  }
  //println(s"Index statistics complete, $queriesTotal queries were executed.")
  
    override def toString = {
      s"""outOut: $outOut
      inOut: $inOut
      inIn: $inIn"""
    }
}
