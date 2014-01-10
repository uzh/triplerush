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
  def optimize(cardinalities: Map[TriplePattern, Int]): List[TriplePattern] = {

    /**
     * takes a list of optimized patterns and one of unoptimized patterns
     * recursively calls itself until the second list is empty
     * moves the most selective pattern from the list of unoptimized patterns to the list of optimized patterns
     * if a query has no solution, returns two empty lists
     */
    @tailrec def optimizePatterns(
      optimizedPatterns: List[TriplePattern],
      unoptimizedPatterns: Set[TriplePattern]): (List[TriplePattern], Set[TriplePattern]) = {

      if (unoptimizedPatterns.isEmpty) {
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
        val costsMap: Map[(TriplePattern, TriplePattern), Int] = {
          for {
            pickedPattern <- unoptimizedPatterns
            costs = costMapForCandidates(pickedPattern, unoptimizedPatterns.filter(_ != pickedPattern))
            (best, costForBest) = costs.minBy(_._2)
          } yield ((pickedPattern, best), costForBest)
        }.toMap
        if (costsMap.values.forall(_ == 0))
          (List(), Set())
        else {
          val ((first, second), bestCost) = costsMap.minBy(_._2)
          (second :: first :: Nil, (unoptimizedPatterns.filter(p => p != first && p != second)))
        }
      } else {
        val costs = costMapForCandidates(optimizedPatterns.head, unoptimizedPatterns)
        val (best, costForBest) = costs.minBy(_._2)
        if (costForBest == 0) {
          (List(), Set())
        } else {
          (best :: optimizedPatterns, unoptimizedPatterns.filter(_ != best))
        }
      }
    }

    /**
     * returns the best pattern to execute next, given the picked pattern
     * if the query has no result, returns none
     */
    def costMapForCandidates(pickedPattern: TriplePattern, candidates: Set[TriplePattern]): Map[TriplePattern, Int] = {
      println(s"comparing $pickedPattern with " + candidates.mkString(" "));
      val expectedExplorationCostForCandidates = candidates.map { candidate =>
        (pickedPattern.s, pickedPattern.o) match {
          case (candidate.s, _) => {
            (candidate, cardinalities(pickedPattern) * predicateSelectivity.outOut(pickedPattern.p, candidate.p))
          }
          case (candidate.o, _) => {
            (candidate, cardinalities(pickedPattern) * predicateSelectivity.outIn(pickedPattern.p, candidate.p))
          }
          case (_, candidate.o) => {
            println("matches _, o")
            (candidate, cardinalities(pickedPattern) * predicateSelectivity.inIn(pickedPattern.p, candidate.p))
          }
          case (_, candidate.s) => {
            (candidate, cardinalities(pickedPattern) * predicateSelectivity.inOut(pickedPattern.p, candidate.p))
          }
          case (_, _) => {
            (candidate, predicateSelectivity.triplesWithPredicate(pickedPattern.p) * predicateSelectivity.triplesWithPredicate(candidate.p))
          }
        }
      }.toMap
      expectedExplorationCostForCandidates
    }
    val (optimized, empty) = optimizePatterns(List(), cardinalities.keySet)
    optimized.reverse
  }
}
