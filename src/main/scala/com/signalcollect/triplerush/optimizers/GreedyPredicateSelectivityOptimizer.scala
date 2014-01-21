package com.signalcollect.triplerush.optimizers
import scala.annotation.tailrec
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern

class GreedyPredicateSelectivityOptimizer(predicateSelectivity: PredicateSelectivity) extends Optimizer {

  def optimize(cardinality: Map[TriplePattern, Int]): Array[TriplePattern] = {

    /**
     * Takes a list of optimized patterns, a set of unoptimized patterns, and
     * recursively calls itself until the set of unoptimized patterns is empty.
     * If a query has no solution, returns an empty list and an empty set.
     */
    @tailrec def optimizePatterns(
      optimizedPatterns: List[TriplePattern],
      unoptimizedPatterns: Set[TriplePattern],
      boundVariables: Set[Int]): (List[TriplePattern], Set[TriplePattern], Set[Int]) = {
      if (unoptimizedPatterns.size == 0) {
        (optimizedPatterns, unoptimizedPatterns, boundVariables)
      } else {
        val (newOpt, newUnopt, newBound) = movePattern(cardinality, optimizedPatterns, unoptimizedPatterns, boundVariables)
        optimizePatterns(newOpt, newUnopt, newBound)
      }
    }

    val (optimized, _, _) = optimizePatterns(List(), cardinality.keySet, Set())
    val bestOrdering = optimized.toArray.reverse
    bestOrdering
  }

  /**
   * Moves the best pattern according to the 'estimateBranchingFactor' function from
   * the unoptimizedPatterns set to the beginning of the optimizedPatterns list and extends
   * the the bound variables set with the variables contained in the moved pattern.
   */
  def movePattern(
    cardinality: Map[TriplePattern, Int],
    optimizedPatterns: List[TriplePattern],
    unoptimizedPatterns: Set[TriplePattern],
    boundVariables: Set[Int]): (List[TriplePattern], Set[TriplePattern], Set[Int]) = {
    if (optimizedPatterns.isEmpty) {
      var bestCost: Option[(TriplePattern, TriplePattern, Long)] = None
      for (first <- unoptimizedPatterns; second <- unoptimizedPatterns) {
        if (first != second) {
          val currentCost = cardinality(first) * estimateBranchingFactor(cardinality, boundVariables, first, second)
          if (bestCost.isEmpty || bestCost.get._3 > currentCost) {
            bestCost = Some((first, second, currentCost))
          }
        }
      }
      val Some((first, second, cost)) = bestCost
      if (cost == 0) {
        (List(), Set(), Set())
      } else {
        (second :: first :: Nil, (unoptimizedPatterns - first - second), first.variableSet ++ second.variableSet)
      }
    } else {
      var bestCost: Option[(TriplePattern, Long)] = None
      for (candidate <- unoptimizedPatterns; explored <- optimizedPatterns) {
        val currentCost = estimateBranchingFactor(cardinality, boundVariables, explored, candidate)
        if (bestCost.isEmpty || bestCost.get._2 > currentCost) {
          bestCost = Some((candidate, currentCost))
        }
      }
      val Some((bestCandidate, cost)) = bestCost
      if (cost == 0) {
        (List(), Set(), Set())
      } else {
        (bestCandidate :: optimizedPatterns, unoptimizedPatterns - bestCandidate, boundVariables ++ bestCandidate.variableSet)
      }
    }
  }

  /**
   * Returns the estimated branching factor of exploring pattern 'next', when pattern 'explored' has already been explored.
   */
  def estimateBranchingFactor(
    cardinality: Map[TriplePattern, Int],
    boundVariables: Set[Int],
    explored: TriplePattern,
    next: TriplePattern): Int = {
    val nextCardinality = cardinality(next)
    val estimatedPredicateBranching = predicateSelectivity.estimateBranchingFactor(explored, next)
    val result = estimatedPredicateBranching.map((math.min(_, nextCardinality))).getOrElse(nextCardinality)
    //println(s"explored=$explored next=$next predSelEst=$estimatedPredicateBranching cardNext=${cardinality(next)} result=$result")
    result
  }

}
