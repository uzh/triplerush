package com.signalcollect.triplerush.optimizers
import scala.annotation.tailrec
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.PredicateStats

class PredicateSelectivityOptimizer(predicateSelectivity: PredicateSelectivity) extends Optimizer {

  /**
   * returns optimal ordering of patterns based on predicate selectivity.
   * TODO: if the optimizer can infer that the query will have no result, then it will return an empty list of patterns
   */

  def optimize(cardinalities: Map[TriplePattern, Long], predicateStats: Map[Int, PredicateStats]): Array[TriplePattern] = {

    println("cardinalities: " + cardinalities.mkString(" "))

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
        val (newOpt, newUnopt, newBound) = movePattern(optimizedPatterns, unoptimizedPatterns, boundVariables)
        optimizePatterns(newOpt, newUnopt, newBound)
      }
    }

    def movePattern(
      optimizedPatterns: List[TriplePattern],
      unoptimizedPatterns: Set[TriplePattern],
      boundVariables: Set[Int]): (List[TriplePattern], Set[TriplePattern], Set[Int]) = {
      if (optimizedPatterns.isEmpty) {
        val costsMap: Map[(TriplePattern, TriplePattern), Long] = {
          for {
            pickedPattern <- unoptimizedPatterns
            costs = costMapForCandidates(List(pickedPattern), unoptimizedPatterns.filter(_ != pickedPattern), Set())
            (best, costForBest) = costs.minBy(_._2)
          } yield ((pickedPattern, best), costForBest)
        }.toMap
        val ((first, second), bestCost) = costsMap.minBy(_._2)
        if (bestCost == 0) {
          (List(), Set(), Set())
        } else {
          (second :: first :: Nil, (unoptimizedPatterns - first - second), first.variableSet ++ second.variableSet)
        }
      } else {
        val costs: Map[(TriplePattern, TriplePattern), Long] = {
          for {
            pickedPattern <- optimizedPatterns
            costs = costMapForCandidates(List(pickedPattern), unoptimizedPatterns, boundVariables)
            (best, costForBest) = costs.minBy(_._2)
          } yield ((pickedPattern, best), costForBest)
        }.toMap

        val ((bestPrev, bestCandidate), costForBest) = costs.minBy(_._2)
        if (costForBest == 0) {
          (List(), Set(), Set())
        } else {
          (bestCandidate :: optimizedPatterns, unoptimizedPatterns - bestCandidate, boundVariables ++ bestCandidate.variableSet)
        }
      }
    }

    /**
     * This method breaks ties between candidate and previous patterns.
     * The aim is to order the pair such that the lower cardinality pattern is followed
     * by a higher cardinality pattern (all other things remaining equal).
     */
    def orderPrevAndCandidate(prev: TriplePattern, candidate: TriplePattern): Int = {
      if (cardinalities(prev) < cardinalities(candidate)) {
        -1
      } else {
        1
      }
    }

    /**
     * Method that removes similar code block inside match block of costForCandidate.
     * Call this method from match block of costForCandidate to calculate cost for candidate.
     */
    def calculateCost(prev: TriplePattern, candidate: TriplePattern, prevBoundOrUnbound: Int, selectivity: Long): Long = {
      val ret =
        if (prevBoundOrUnbound > 0)
          math.max(selectivity, cardinalities(candidate) + cardinalities(prev)) + orderPrevAndCandidate(prev, candidate)
        else if (selectivity == -1)
          cardinalities(prev) * cardinalities(candidate)
        else cardinalities(prev) * selectivity
      if (ret < 0)
        Long.MaxValue
      else ret
    }

    /**
     * Given a previously picked pattern and a candidate, return the cost for this pair
     * Use heuristics such as if the previous pattern has a common variable, and the other one is a constant, etc.
     */
    def costForCandidate(prev: TriplePattern, candidate: TriplePattern): Long = {
      val upperBoundBasedOnPredicateSelectivity = (prev.s, prev.o) match {
        case (candidate.s, _) =>
          calculateCost(prev, candidate, prev.o, predicateSelectivity.outOut(prev.p, candidate.p))
        case (candidate.o, _) =>
          calculateCost(prev, candidate, prev.o, predicateSelectivity.outIn(prev.p, candidate.p))
        case (_, candidate.o) =>
          calculateCost(prev, candidate, prev.s, predicateSelectivity.inIn(prev.p, candidate.p))
        case (_, candidate.s) =>
          calculateCost(prev, candidate, prev.s, predicateSelectivity.inOut(prev.p, candidate.p))
        case other =>
          val ret =
            cardinalities(prev) * cardinalities(candidate) + orderPrevAndCandidate(prev, candidate)
          if (ret < 0) {
            Long.MaxValue
          } else {
            ret
          }

      }

      math.min(upperBoundBasedOnPredicateSelectivity, Long.MaxValue)
    }

    /**
     * Calculate cost for candidate whose variables are already bound.
     */
    def costForBoundCandidate(prev: TriplePattern, candidate: TriplePattern): Long = {
      // If variables are bound, we want to order such that the least cost candidate is ordered after the previous patterns and
      // we need to break ties as well.
      val vars = (Set[Int]() + prev.s + prev.o).filter(_ < 0)
      val upperBoundOnSelectivity: Long = {
        if (vars.contains(candidate.s) || vars.contains(candidate.o)) {
          if (cardinalities(prev) > cardinalities(candidate)) {
            cardinalities(prev)
          } else {
            cardinalities(candidate)
          }
        } else {
          Long.MaxValue
        }
      }
      math.min(upperBoundOnSelectivity, Long.MaxValue)
    }

    /**
     * Returns a cost map for all candidates.
     */
    def costMapForCandidates(pickedPatterns: List[TriplePattern], candidates: Set[TriplePattern], boundVariables: Set[Int]): Map[TriplePattern, Long] = {
      candidates.map { candidate =>
        val bestCost = {
          //if candidate's variables are already bound, calculate cost using a simple heuristic
          if ((!boundVariables.isEmpty) &&
            ((candidate.o > 0 && boundVariables.contains(candidate.s)) ||
              (candidate.s > 0 && boundVariables.contains(candidate.o)) ||
              (boundVariables.contains(candidate.s) && boundVariables.contains(candidate.o))))
            pickedPatterns.map(costForBoundCandidate(_, candidate)).min
          else pickedPatterns.map(costForCandidate(_, candidate)).min
        }
        (candidate, bestCost)
      }.toMap
    }

    val (optimized, _, _) = optimizePatterns(List(), cardinalities.keySet, Set())
    optimized.toArray.reverse
  }
}
