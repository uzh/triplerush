package com.signalcollect.triplerush

import scala.Array.canBuildFrom
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec

class PredicateSelectivityOptimizer(predicateSelectivity: PredicateSelectivity, debug: Boolean) extends Optimizer {

  /**
   * returns optimal ordering of patterns based on predicate selectivity.
   * TODO: if the optimizer can infer that the query will have no result, then it will return an empty list of patterns
   */
  def optimize(cardinalities: Map[TriplePattern, Int]): Array[TriplePattern] = {

    /**
     * takes a list of optimized patterns and one of unoptimized patterns
     * recursively calls itself until the second list is empty
     * moves the most selective pattern from the list of unoptimized patterns to the list of optimized patterns
     * if a query has no solution, returns two empty lists
     */
    @tailrec def optimizePatterns(
      optimizedPatterns: List[TriplePattern],
      unoptimizedPatterns: Set[TriplePattern]): (List[TriplePattern], Set[TriplePattern]) = {

      if (unoptimizedPatterns.size == 0) {
        (optimizedPatterns, unoptimizedPatterns)
      } else {
        val (newOpt, newUnopt) = movePattern(optimizedPatterns, unoptimizedPatterns)
        optimizePatterns(newOpt, newUnopt)
      }
    }

    def movePattern(
      optimizedPatterns: List[TriplePattern],
      unoptimizedPatterns: Set[TriplePattern]): (List[TriplePattern], Set[TriplePattern]) = {
      if (optimizedPatterns.isEmpty) {
        val costsMap: Map[(TriplePattern, TriplePattern), Long] = {
          for {
            pickedPattern <- unoptimizedPatterns
            costs = costMapForCandidates(List(pickedPattern), unoptimizedPatterns.filter(_ != pickedPattern))
            (best, costForBest) = costs.minBy(_._2)
          } yield ((pickedPattern, best), costForBest * cardinalities(pickedPattern))
        }.toMap
        if (costsMap.values.forall(_ == 0))
          (List(), Set())
        else {
          val ((first, second), bestCost) = costsMap.minBy(_._2)
          (second :: first :: Nil, (unoptimizedPatterns.filter(p => p != first && p != second)))
        }
      } else {
        val costs = costMapForCandidates(optimizedPatterns, unoptimizedPatterns)
        val (best, costForBest) = costs.minBy(_._2)
        if (costForBest == 0) {
          (List(), Set())
        } else {
          (best :: optimizedPatterns, unoptimizedPatterns.filter(_ != best))
        }
      }
    }

    def costForCandidate(prev: TriplePattern, candidate: TriplePattern): Long = {
      val upperBoundBasedOnPredicateSelectivity = (prev.s, prev.o) match {
        case (candidate.s, _) => predicateSelectivity.outOut(prev.p, candidate.p)
        case (candidate.o, _) => predicateSelectivity.outIn(prev.p, candidate.p)
        case (_, candidate.o) => predicateSelectivity.inIn(prev.p, candidate.p)
        case (_, candidate.s) => predicateSelectivity.inOut(prev.p, candidate.p)
        case other => Long.MaxValue
      }
      math.min(upperBoundBasedOnPredicateSelectivity, cardinalities(candidate))
    }

    /**
     * returns a cost map for all candidates
     */
    def costMapForCandidates(pickedPatterns: List[TriplePattern], candidates: Set[TriplePattern]): Map[TriplePattern, Long] = {
      candidates.map { candidate =>
        val bestCost = pickedPatterns.map(costForCandidate(_, candidate)).min
        (candidate, bestCost)
      }.toMap
    }
    val (optimized, empty) = optimizePatterns(List(), cardinalities.keySet)
    optimized.toArray.reverse
  }
}
