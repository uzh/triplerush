package com.signalcollect.triplerush.optimizers

import scala.annotation.tailrec
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.PredicateStats

class AdvancedOptimizer(pairwisePatternCardinalities: Map[Set[TriplePattern], Option[Long]]) extends Optimizer {

  def optimize(cardinalities: Map[TriplePattern, Long], predicateStats: Map[Int, PredicateStats]): Array[TriplePattern] = {

    /**
     * Takes a list of optimized patterns, a set of unoptimized patterns, and
     * recursively calls itself until the set of unoptimized patterns is empty.
     * If a query has no solution, returns an empty list and an empty set.
     */
    @tailrec def optimizePatterns(
      optimizedPatterns: List[TriplePattern],
      unoptimizedPatterns: Set[TriplePattern],
      bestCostEstimateTable: Map[TriplePattern, Long],
      boundVariables: Set[Int]): (List[TriplePattern], Set[TriplePattern], Set[Int]) = {
      if (unoptimizedPatterns.size == 0) {
        (optimizedPatterns, unoptimizedPatterns, boundVariables)
      } else {
        val (newOpt, newUnopt, newCostEstimateTable, newBound) = movePattern(optimizedPatterns, unoptimizedPatterns, bestCostEstimateTable, boundVariables)
        optimizePatterns(newOpt, newUnopt, newCostEstimateTable, newBound)
      }
    }

    val (optimized, _, _) = optimizePatterns(List(), cardinalities.keySet, cardinalities, Set())
    val bestOrdering = optimized.toArray.reverse
    bestOrdering
  }

  def refineEstimate(pattern: TriplePattern, baseEstimate: Long, boundVariables: Set[Int]): Long = {
    val patternVariables = pattern.variableSet
    val unboundVariables = patternVariables -- boundVariables
    val variablesBoundSinceBaseEstimates = patternVariables.intersect(boundVariables).size
    if (baseEstimate == 0) {
      0
    } else if (unboundVariables == 0) {
      1
    } else if (variablesBoundSinceBaseEstimates == 1) {
      (baseEstimate / 100) + 1
    } else if (variablesBoundSinceBaseEstimates == 2) {
      (baseEstimate / 10000) + 1
    } else {
      baseEstimate
    }
  }

  /**
   * Moves the best pattern from the unoptimizedPatterns set to the beginning of
   * the optimizedPatterns list and extends the the bound variables set with the
   * variables contained in the moved pattern.
   */
  def movePattern(
    optimizedPatterns: List[TriplePattern],
    unoptimizedPatterns: Set[TriplePattern],
    costEstimateTable: Map[TriplePattern, Long],
    boundVariables: Set[Int]): (List[TriplePattern], Set[TriplePattern], Map[TriplePattern, Long], Set[Int]) = {
    if (optimizedPatterns.isEmpty) {
      var best: Option[(TriplePattern, TriplePattern, Long)] = None
      for (
        first <- unoptimizedPatterns;
        firstVariables = first.variableSet;
        second <- unoptimizedPatterns
      ) {
        if (first != second) {
          val baseEstimateSecond = math.min(
            costEstimateTable(first),
            pairwisePatternCardinalities(Set(first, second)).getOrElse(Long.MaxValue))
          val refinedSecondEstimate = refineEstimate(second, baseEstimateSecond, firstVariables)
          val currentCost: Long = costEstimateTable(first).toLong * refinedSecondEstimate
          if (best.isEmpty || best.get._3 > currentCost) {
            best = Some((first, second, currentCost))
          }
        }
      }
      val Some((first, second, bestCost)) = best
      if (bestCost == 0) {
        (List(), Set(), Map(), Set())
      } else {
        val newlyBoundVariables = first.variableSet ++ second.variableSet
        val updatedBestCostTable = costEstimateTable.flatMap {
          case (pattern, oldEstimate) =>
            if (pattern == first || pattern == second) {
              None
            } else {
              val withFirst = pairwisePatternCardinalities(Set(first, pattern)).getOrElse(Long.MaxValue)
              val withSecond = pairwisePatternCardinalities(Set(second, pattern)).getOrElse(Long.MaxValue)
              val bestBaseEstimate = math.min(math.min(oldEstimate, withFirst), withSecond)
              Some((pattern, bestBaseEstimate))
            }
        }
        (second :: first :: Nil, (unoptimizedPatterns - first - second), updatedBestCostTable, newlyBoundVariables)
      }
    } else {
      var best: Option[(TriplePattern, Long)] = None
      for (candidate <- unoptimizedPatterns) {
        val refinedEstimate = refineEstimate(candidate, costEstimateTable(candidate), boundVariables)
        if (best.isEmpty || best.get._2 > refinedEstimate) {
          best = Some((candidate, refinedEstimate))
        }
      }
      val Some((bestCandidate, bestCost)) = best
      if (bestCost == 0) {
        (List(), Set(), Map(), Set())
      } else {
        val updatedBestCostTable = costEstimateTable.flatMap {
          case (pattern, oldEstimate) =>
            if (pattern == bestCandidate) {
              None
            } else {
              val patternCard = pairwisePatternCardinalities(Set(bestCandidate, pattern)).getOrElse(Long.MaxValue)
              val bestBaseEstimate = math.min(oldEstimate, patternCard)
              Some((pattern, bestBaseEstimate))
            }
        }
        (bestCandidate :: optimizedPatterns, unoptimizedPatterns - bestCandidate, updatedBestCostTable, boundVariables ++ bestCandidate.variableSet)
      }
    }
  }

}
