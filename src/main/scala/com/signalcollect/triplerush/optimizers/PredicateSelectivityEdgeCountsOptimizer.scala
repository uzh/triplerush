package com.signalcollect.triplerush.optimizers
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern

class PredicateSelectivityEdgeCountsOptimizer(predicateSelectivity: PredicateSelectivity) extends Optimizer {
  /**
   * returns optimal ordering of patterns based on predicate selectivity.
   * TODO: if the optimizer can infer that the query will have no result, then it will return an empty list of patterns
   */
  
  def optimize(cardinalities: Map[TriplePattern, Long], edgeCounts: Option[Map[TriplePattern, Long]]): Array[TriplePattern] = {

    def reverseMutableArray(arr: Array[TriplePattern]) {
      var fromStart = 0
      var fromEnd = arr.length - 1
      while(fromStart < fromEnd){
        val t = arr(fromStart)
        arr(fromStart) = arr(fromEnd)
        arr(fromEnd) = t
        fromStart += 1
        fromEnd -= 1
      }
    }
    
    def generateCombinations(ps: Array[TriplePattern]): Array[(TriplePattern, Set[TriplePattern])] = {
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
    def costOfCombination(patterns: Array[TriplePattern], lookup: Map[Set[TriplePattern], (List[TriplePattern], Double, Double, Double)]): (List[TriplePattern], Double, Double, Double) = {

      if (patterns.length == 1) {
        val card = cardinalities(patterns(0))
        (patterns.toList, card, card, card)
      } else {
        val splits: Array[(TriplePattern, Set[TriplePattern])] = generateCombinations(patterns)
        println(s"\tsplits: ${splits.mkString(" ")}")
        val minCostPossibilities = for (
          disjointOrder <- splits
        ) yield (costOfPatternGivenPrevious(disjointOrder._1, lookup(disjointOrder._2)))
        
        println(s"\tminCostPossibilities: ${minCostPossibilities.mkString(" ")}")
        
        val bestCost = minCostPossibilities.minBy(_._4)._4
        val multiplePatternListWithBestCost = minCostPossibilities.filter(_._4 == bestCost)
        val patternListWithBestCost = {
          if (multiplePatternListWithBestCost.size > 1)
            multiplePatternListWithBestCost.min(OrderingOfListOfTriplePattern)
          else
            minCostPossibilities.minBy(_._4)
        }
        if (bestCost == 0) {
          (patternListWithBestCost._1, 0, 0, 0)
        } else {
          patternListWithBestCost
        }
      }
    }

    implicit object OrderingOfListOfTriplePattern extends Ordering[(List[TriplePattern], Double, Double, Double)] {
      def compare(one: (List[TriplePattern], Double, Double, Double), other: (List[TriplePattern], Double, Double, Double)): Int = {
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
    def costOfPatternGivenPrevious(candidate: TriplePattern, previous: (List[TriplePattern], Double, Double, Double)): (List[TriplePattern], Double, Double, Double) = {
      val cost: (List[TriplePattern], Double, Double, Double) = {
        val res = costForPattern(candidate, previous)
        if (res._3 == 0) {
          (candidate :: previous._1, 0, 0, 0)
        } else {
          (candidate :: previous._1, res._1, res._2, res._3)
          //(candidate :: previous._1, res._1 + previous._2, res._2 + previous._3, res._3 + previous._4)
          //(candidate :: previous._1, res._1 + previous._2, res._2 + previous._3, res._1 + previous._2 + res._2 + previous._3)
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
    def costForPattern(candidate: TriplePattern, previous: (List[TriplePattern], Double, Double, Double)): (Double, Double, Double) = {
      println(s"\tcost for candidate: $candidate, previous: ${previous._1.mkString(" ")}")
      
      val exploreCost = previous._2 * exploreCostForCandidatePattern(candidate, previous._1)
      println(s"\texploreCost: f(-1) * explore: $exploreCost")
      val frontierSize = frontierSizeForCandidatePattern(candidate, exploreCost, previous._1)
      println(s"\tfrontier: $frontierSize")
      if (frontierSize == 0) {
        (0, 0, 0)
      } else {
        (frontierSize, exploreCost, exploreCost)
      }
    }

    /**
     * returns lookupcost for the candidate pattern, given the cost of previous pattern order
     */
    def exploreCostForCandidatePattern(candidate: TriplePattern, pickedPatterns: List[TriplePattern]): Double = {
      val boundVariables = pickedPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }
      val intersectionVariables = boundVariables.intersect(candidate.variableSet)
      //if all bound
      if ((intersectionVariables.size == candidate.variableSet.size) && candidate.p > 0) {
        println("\t  explore: everthing bound: 1")
        1
      } //if o,p bound
      else if ((candidate.o > 0 || intersectionVariables.contains(candidate.o)) && candidate.p > 0) {
        println(s"\t  explore: o,p bound, min(card, edgeCount): ${math.min(cardinalities(candidate), edgeCounts.get(TriplePattern(0, candidate.p, 0)))}")
        math.min(cardinalities(candidate), edgeCounts.get(TriplePattern(0, candidate.p, 0)))
      } //if s,p bound
      else if ((candidate.s > 0 || intersectionVariables.contains(candidate.s)) && candidate.p > 0) {
        val numberOfSubjects = 1 + (cardinalities(candidate) / edgeCounts.get(TriplePattern(0, candidate.p, 0)))
        println(s"\t  explore: s,p bound, min(card, card/edgeCount): ${math.min(cardinalities(candidate), numberOfSubjects)}")
        math.min(cardinalities(candidate), numberOfSubjects)
      } else {
        println(s"\t  explore: nothing bound, card: ${cardinalities(candidate)}")
        cardinalities(candidate)
      }
    }

    /**
     * returns frontierSize for the candidate pattern, given the cost of previous pattern order and previous pattern order
     */
    def frontierSizeForCandidatePattern(candidate: TriplePattern, exploreCostOfCandidate: Double, pickedPatterns: List[TriplePattern]): Double = {
      val boundVariables = pickedPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }

      //if either s or o is bound
      if ((candidate.o > 0 || candidate.s > 0 || boundVariables.contains(candidate.s) || boundVariables.contains(candidate.o)) && (candidate.p > 0)) {
        val minimumPredicateSelectivityCost = pickedPatterns.map { prev => calculatePredicateSelectivityCost(prev, candidate) }.min
        println(s"\t  frontier: s or o bound, min(explore, selectivity($minimumPredicateSelectivityCost)): ${math.min(exploreCostOfCandidate, minimumPredicateSelectivityCost)}")
        math.min(exploreCostOfCandidate, minimumPredicateSelectivityCost)
      } //otherwise
      else {
        println(s"\t  frontier: nothing bound, explore $exploreCostOfCandidate")
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
    val patternWindows = for (
      size <- 1 to triplePatterns.size;
      window <- triplePatterns.combinations(size)
    ) yield window

    /**
     * create a lookup table for optimal ordering and cost for each plan
     */
    val lookupTable = patternWindows.foldLeft(Map.empty[Set[TriplePattern], (List[TriplePattern], Double, Double, Double)]) { case (result, current) => result + (current.toSet -> costOfCombination(current, result)) }

    /**
     * the optimal ordering for the query is the corresponding entry in the lookup table for the set of all patterns
     */
    val optimalCombination = lookupTable(triplePatterns.toSet)
    val optimalOrder = {
      if (optimalCombination._4 == 0)
        List()
      else
        optimalCombination._1
    }

    val resultOrder = optimalOrder.toArray
    reverseMutableArray(resultOrder)
    
    println(s"\tALL ORDERS: ${lookupTable.mkString("\n\t")}")
    println(s"optimal order: ${resultOrder.mkString(" ")}")
    println(s"cost of optimal order: ${optimalCombination._2}, ${optimalCombination._3}, ${optimalCombination._4}")
    println(s"cardinalities: ${cardinalities.mkString(" ")}")
    println("edgeCounts: " + edgeCounts.mkString(" ") + "\n")
    
    resultOrder
  }
}
