package com.signalcollect.triplerush.optimizers
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import scala.annotation.tailrec
import com.signalcollect.triplerush.PredicateStats

final class PredicateSelectivityEdgeCountsOptimizer(predicateSelectivity: PredicateSelectivity) extends Optimizer {
  /**
   * returns optimal ordering of patterns based on predicate selectivity.
   * TODO: if the optimizer can infer that the query will have no result, then it will return an empty list of patterns
   */

  case class CostEstimate(frontier: Double, lastExploration: Double, explorationSum: Double)

  def optimize(cardinalities: Map[TriplePattern, Long], predicateStats: Map[Int, PredicateStats]): Array[TriplePattern] = {

    @inline def reverseMutableArray(arr: Array[TriplePattern]) {
      var fromStart = 0
      var fromEnd = arr.length - 1
      while (fromStart < fromEnd) {
        val t = arr(fromStart)
        arr(fromStart) = arr(fromEnd)
        arr(fromEnd) = t
        fromStart += 1
        fromEnd -= 1
      }
    }

    @inline def generateCombinations(ps: Array[TriplePattern]): Array[(TriplePattern, Set[TriplePattern])] = {
      val s = ps.toSet
      for (item <- ps) yield (item, s - item)
    }

    /**
     * parameters:
     * 		list of k patterns (patternOrder)
     *   	lookup table for cost of pattern orders of length j = 2 to k
     *    		lookup table contains for each list of j-length pattern: the optimal ordering of the patterns, and the following values: size of frontier, exploration cost, totalcost
     * returns:
     * 		optimal ordering for the list of k-length pattern, and the following values: size of frontier, exploration cost, totalcost
     */
    def costOfCombination(patterns: Array[TriplePattern], lookup: Map[Set[TriplePattern], (List[TriplePattern], CostEstimate)]): (List[TriplePattern], CostEstimate) = {

      if (patterns.length == 1) {
        val card = cardinalities(patterns(0))
        val exploreCost = exploreCostForCandidatePattern(patterns(0), List())
        val frontier = frontierSizeForCandidatePattern(patterns(0), exploreCost, List())
        (patterns.toList, CostEstimate(frontier, exploreCost, exploreCost))
      } else {
        val splits: Array[(TriplePattern, Set[TriplePattern])] = generateCombinations(patterns)
        val minCostPossibilities = for (
          disjointOrder <- splits
        ) yield (costOfPatternGivenPrevious(disjointOrder._1, lookup(disjointOrder._2)))

        val bestCost = minCostPossibilities.minBy(_._2.explorationSum)._2.explorationSum
        val multiplePatternListWithBestCost = minCostPossibilities.filter(_._2.explorationSum == bestCost)
        val patternListWithBestCost = {
          if (multiplePatternListWithBestCost.size > 1) {
            multiplePatternListWithBestCost.min(OrderingOfListOfTriplePattern)
          } else {
            minCostPossibilities.minBy(_._2.explorationSum)
          }
        }
        if (bestCost == 0) {
          (patternListWithBestCost._1, CostEstimate(0, 0, 0))
        } else {
          patternListWithBestCost
        }
      }
    }

    implicit object OrderingOfListOfTriplePattern extends Ordering[(List[TriplePattern], CostEstimate)] {
      def compare(one: (List[TriplePattern], CostEstimate), other: (List[TriplePattern], CostEstimate)): Int = {
        if (cardinalities(one._1(0)) == cardinalities(other._1(0))) {
          cardinalities(one._1(1)).compare(cardinalities(other._1(1)))
        } else
          cardinalities(one._1(0)).compare(cardinalities(other._1(0)))
      }
    }

    /**
     * parameters:
     * 		list of candidate pattern (length 1 or 2)
     *   	Optional list of previous optimal pattern order and the corresponding costs: size of frontier, exploration cost, totalcost
     * returns:
     * 		optimal ordering of candidate with previously picked patterns, together with the corresponding costs: size of frontier, exploration cost, totalcost
     */
    def costOfPatternGivenPrevious(candidate: TriplePattern, previous: (List[TriplePattern], CostEstimate)): (List[TriplePattern], CostEstimate) = {
      val cost: (List[TriplePattern], CostEstimate) = {
        val res = costForPattern(candidate, previous)
        if (res.lastExploration == 0) {
          (candidate :: previous._1, CostEstimate(0, 0, 0))
        } else {
          (candidate :: previous._1, CostEstimate(res.frontier, res.lastExploration, res.explorationSum + previous._2.explorationSum))
        }
      }
      cost
    }

    /**
     * parameters:
     * 		candidate pattern
     *   	previously picked pattern order with the corresponding costs: size of frontier, exploration cost, totalcost
     * returns:
     * 		cost of the order: size of frontier, exploration cost, totalcost
     */
    def costForPattern(candidate: TriplePattern, previous: (List[TriplePattern], CostEstimate)): CostEstimate = {
      val exploreCost = previous._2.frontier * exploreCostForCandidatePattern(candidate, previous._1)
      val frontierSize = frontierSizeForCandidatePattern(candidate, exploreCost, previous._1)
      if (frontierSize == 0) {
        CostEstimate(0, 0, 0)
      } else {
        CostEstimate(frontierSize, exploreCost, exploreCost)
      }
    }

    /**
     * returns lookupcost for the candidate pattern, given the cost of previous pattern order
     */
    def exploreCostForCandidatePattern(candidate: TriplePattern, pickedPatterns: List[TriplePattern]): Double = {
      val boundVariables = pickedPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }
      val intersectionVariables = boundVariables.intersect(candidate.variableSet)
      val numberOfPredicates = predicateSelectivity.predicates.size
      val predicateIndexForCandidate = candidate.p
      val isSubjectBound = (candidate.s > 0 || intersectionVariables.contains(candidate.s))
      val isObjectBound = (candidate.o > 0 || intersectionVariables.contains(candidate.o))

      if (candidate.p > 0) {
        val stats = predicateStats(predicateIndexForCandidate)
        //if all bound
        if ((intersectionVariables.size == candidate.variableSet.size)) {
          1
        } //s,p,*
        else if (isSubjectBound && candidate.o < 0) {
          math.min(cardinalities(candidate), stats.objectCount)
        } //*,p,o
        else if (isObjectBound && candidate.s < 0) {
          math.min(cardinalities(candidate), stats.subjectCount)
        } //s,*,o
        else if (isSubjectBound && isObjectBound) {
          math.min(cardinalities(candidate), numberOfPredicates)
        } //*,p,*
        else if (!isSubjectBound && !isObjectBound) {
          math.min(cardinalities(candidate), stats.edgeCount * stats.objectCount)
        } else {
          cardinalities(candidate)
        }
      } else {
        cardinalities(candidate)
      }
    }

    /**
     * returns frontierSize for the candidate pattern, given the cost of previous pattern order and previous pattern order
     */
    def frontierSizeForCandidatePattern(candidate: TriplePattern, exploreCostOfCandidate: Double, pickedPatterns: List[TriplePattern]): Double = {
      val boundVariables = pickedPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }

      if (pickedPatterns.isEmpty) {
        exploreCostOfCandidate
      } //if either s or o is bound)
      else if ((candidate.o > 0 || candidate.s > 0 || boundVariables.contains(candidate.s) || boundVariables.contains(candidate.o)) && (candidate.p > 0)) {
        val minimumPredicateSelectivityCost = {
          pickedPatterns.map { prev => if(prev.p < 0) Double.MaxValue else calculatePredicateSelectivityCost(prev, candidate) }.min
        }
        math.min(exploreCostOfCandidate, minimumPredicateSelectivityCost)
      } //otherwise
      else {
        exploreCostOfCandidate
      }
    }

    def calculatePredicateSelectivityCost(prev: TriplePattern, candidate: TriplePattern): Double = {
      val upperBoundBasedOnPredicateSelectivity = (prev.s, prev.o) match {
        case (candidate.s, _) =>
          predicateSelectivity.outOut(prev.p, candidate.p)
        case (candidate.o, _) =>
          predicateSelectivity.outIn(prev.p, candidate.p)
        case (_, candidate.o) =>
          predicateSelectivity.inIn(prev.p, candidate.p)
        case (_, candidate.s) =>
          predicateSelectivity.inOut(prev.p, candidate.p)
        case other =>
          Double.MaxValue
      }
      
      upperBoundBasedOnPredicateSelectivity
    }

    val triplePatterns = cardinalities.keys.toArray
    

    /**
     * find all plans whose cost have to be calculated
     * we'll store them in a lookup table
     */
    val patternWindows = for {
      size <- 1 to triplePatterns.size
      window <- triplePatterns.combinations(size)
    } yield window

    /**
     * create a lookup table for optimal ordering and cost for each plan
     */
    val lookupTable = patternWindows.foldLeft(Map.empty[Set[TriplePattern], (List[TriplePattern], CostEstimate)]) {
      case (result, current) => result + (current.toSet -> costOfCombination(current, result))
    }

    /**
     * the optimal ordering for the query is the corresponding entry in the lookup table for the set of all patterns
     */
    val optimalCombination = lookupTable(triplePatterns.toSet)
    val optimalOrder = {
      if (optimalCombination._2.explorationSum == 0)
        List()
      else
        optimalCombination._1
    }

    val resultOrder = optimalOrder.toArray
    reverseMutableArray(resultOrder)

    
    //println(s"optimal order: ${resultOrder.mkString(" ")}")
    //println(s"cardinalities: ${cardinalities.mkString(" ")}")
    //println("edgeCounts: " + stats.edgeCounts.mkString(" "))
    //println("maxObjectCounts: " + stats.maxObjectCounts.mkString(" "))
    //println("maxSubjectCounts: " + stats.maxSubjectCounts.mkString(" ") + "\n")
	

    resultOrder
  }
}
