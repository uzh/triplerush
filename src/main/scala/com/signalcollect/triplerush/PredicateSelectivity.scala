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

  val predicates = tr.childIdsForPattern(EfficientIndexPattern(0, 0, 0))

  val ps = predicates.size
  //println(s"Computing selectivities for $ps * $ps = ${ps * ps} predicate combinations ...")

  private var _outOut = Map[(Int, Int), Long]()
  private var _inOut = Map[(Int, Int), Long]()
  private var _inIn = Map[(Int, Int), Long]()
  
  def outOut(p1: Int, p2: Int) = _outOut((p1, p2))
  def inOut(p1: Int, p2: Int) = _inOut((p1, p2))
  def inIn(p1: Int, p2: Int) = _inIn((p1, p2))
  def outIn(p1: Int, p2: Int) = _inOut((p2, p1))

  val optimizer = Some(GreedyCardinalityOptimizer)
  val queriesTotal = ps * ps * 3
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
      val outOutQuery = Seq(TriplePattern(s, p1, x), TriplePattern(s, p2, y))
      val outOutResult = tr.executeCountingQuery(outOutQuery, optimizer)
      val inOutQuery = Seq(TriplePattern(x, p1, o), TriplePattern(o, p2, y))
      val inOutResult = tr.executeCountingQuery(inOutQuery, optimizer)
      val inInQuery = Seq(TriplePattern(x, p1, o), TriplePattern(y, p2, o))
      val inInResult = tr.executeCountingQuery(inInQuery, optimizer)

      // TODO: Handle the else parts better.
      // TODO: Figure out worst case for selectivity of join. 
      // If worst case count is more than 100 billion (100,000,000,000), then don't execute the query but instead assume Long.MAX

      val outOutResultSize =
        try {
          Await.result(outOutResult, 120.seconds).get
        } catch {
          case e: Exception =>
            Long.MaxValue
        }

      val inOutResultSize =
        try {
          Await.result(inOutResult, 120.seconds).get
        } catch {
          case e: Exception =>
            Long.MaxValue
        }

      val inInResultSize =
        try {
          Await.result(inInResult, 120.seconds).get
        } catch {
          case e: Exception =>
            Long.MaxValue
        }

      _outOut += (p1, p2) -> outOutResultSize
      _inOut += (p1, p2) -> inOutResultSize
      _inIn += (p1, p2) -> inInResultSize

      queriesSoFar += 3
    }
  }
  //println(s"Index statistics complete, $queriesTotal queries were executed.")

  override def toString = {
    s"""outOut: ${_outOut}
      inOut: ${_inOut}
      inIn: ${_inIn}"""
  }
}
