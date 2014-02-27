package com.signalcollect.triplerush.optimizers
import scala.annotation.tailrec
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

    //println("cardinalities: " + cardinalities.mkString(" ") + ", edgeCounts: " + edgeCounts.mkString(" "))

    def generateCombinations(ps: Array[TriplePattern]): List[(TriplePattern, Set[TriplePattern])] = {
//      for (i <- 0 to ps.length) {
//        
//      }
      ???
    }
    
    /**
     * parameters:
     * 		list of k patterns (patternOrder)
     *   	lookup table for cost of pattern orders of length j = 2 to k
     *    		lookup table contains for each list of j-length pattern: the optimal ordering of the patterns, and the following values: size of frontier, exploration cost, totalcost
     * returns:
     * 		optimal ordering for the list of k-length pattern, and the following values: size of frontier, exploration cost, totalcost
     */
    def costOfCombination(patterns: Array[TriplePattern], lookup: Map[Set[TriplePattern], Option[(List[TriplePattern], Double, Double, Double)]]): Option[(List[TriplePattern], Double, Double, Double)] = {
      val orderings = patterns.permutations
//      val patternOrderOfLesserSize = patternOrder.map(x => List(x) ::: (patternOrder diff List(x)))
//      val splits = {
//        if (patternOrder.size > 2)
//          patternOrderOfLesserSize.map(x => x.splitAt(1))
//        else patternOrderOfLesserSize.map(x => x.splitAt(2))
//      }
//
//      //println(s"\tsplits: ${splits.mkString(", ")}")
//
//      val minCostPossibilities = for (
//        disjointOrder <- splits
//      ) yield (costOfPatternGivenPrevious(disjointOrder._1, lookup.getOrElse(disjointOrder._2.toSet, None)))
//
//      val bestCost = minCostPossibilities.minBy(_._4)._4
//      /*if (bestCost == 0){
//        println("ZERO value for this combination")
//        (Some(List(), 0, 0, 0))
//      }
//      else {*/
//      val multiplePatternListWithBestCost = minCostPossibilities.filter(_._4 == bestCost)
//      val patternListWithBestCost = {
//        if (multiplePatternListWithBestCost.size > 1)
//          multiplePatternListWithBestCost.min(OrderingOfListOfTriplePattern)
//        else
//          minCostPossibilities.minBy(_._4)
//      }
//
//      //val minPair = minCostPossibilities.minBy(_._4)
//      /*
//      println(s"\tcosts: ${patternListWithBestCost}")
//      print(s"\tmin: ${patternListWithBestCost._1}")
//      for (p <- patternListWithBestCost._1)
//        print(s", ${cardinalities(p)} (${edgeCounts.get(TriplePattern(0, p.p, 0))})")
//      println("\n")
//	*/
//      if (bestCost == 0) {
//        //println("ZERO value for this combination")
//        (Some(patternListWithBestCost._1, 0, 0, 0))
//      }
//      Some(patternListWithBestCost)
//      //}
      ???
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
    def costOfPatternGivenPrevious(candidate: List[TriplePattern], previous: Option[(List[TriplePattern], Double, Double, Double)]): (List[TriplePattern], Double, Double, Double) = {
      val cost: (List[TriplePattern], Double, Double, Double) = previous match {
        case Some(pattern) => {
          //println(s"\t  candidate: ${candidate.mkString(", ")}, previous: ${previous.get._1.mkString(", ")}")
          val res = costForPattern(candidate(0), previous.get)
          //TODO: check this
          if (res._3 == 0)
            (previous.get._1 ::: List(candidate(0)), 0, 0, 0)
          else //(previous.get._1 ::: List(candidate(0)), res._1 , res._2 , res._3 )
            (previous.get._1 ::: List(candidate(0)), res._1 + previous.get._2, res._2 + previous.get._3, res._3 + previous.get._4)
            //(previous.get._1 ::: List(candidate(0)), res._1, res._2, res._3 + previous.get._4)
            //(previous.get._1 ::: List(candidate(0)), res._1, res._2 + previous.get._3, res._3)
        }
        case None => {
          //println(s"\t  candidate: ${candidate.mkString(", ")}, previous: None")
          val res = costForPatternPair(candidate)
          if (res._3 == 0)
            (candidate, 0, 0, 0)
          else (candidate, res._1, res._2, res._3)
        }
      }
      cost
    }

    /**
     * parameters:
     * 		list of two triplepatterns (ordered)
     * returns:
     * 		cost of the order: size of frontier, exploration cost, totalcost
     */
    def costForPatternPair(pair: List[TriplePattern]): (Double, Double, Double) = {
      val exploreCostFor1 = cardinalities(pair(0))
      val frontierSizeFor1 = exploreCostFor1
      val exploreCostFor2 = frontierSizeFor1 * exploreCostForCandidatePattern(pair(1), List(pair(0)))
      //println(s"\t\tpair explore: $exploreCostFor2")
      val frontierSizeFor2 = frontierSizeForCandidatePattern(pair(1), exploreCostFor2, List(pair(0)))
      if (frontierSizeFor2 == 0) {
        //println("\t\tZERO value returned")
        (0, 0, 0)
      } //println(s"\t\tpair frontier: $frontierSizeFor2")
      else
        //(frontierSizeFor2, exploreCostFor2, exploreCostFor2)
        //(frontierSizeFor2 + frontierSizeFor1, exploreCostFor2 + exploreCostFor1, exploreCostFor2 + exploreCostFor1)
        (frontierSizeFor2, exploreCostFor2, exploreCostFor2 + exploreCostFor1)
    }

    /**
     * parameters:
     * 		candidate pattern
     *   	previously picked pattern order with the corresponding costs: size of frontier, exploration cost, totalcost
     * returns:
     * 		cost of the order: size of frontier, exploration cost, totalcost
     */
    def costForPattern(candidate: TriplePattern, previous: (List[TriplePattern], Double, Double, Double)): (Double, Double, Double) = {
      val exploreCost = previous._2 * exploreCostForCandidatePattern(candidate, previous._1)
      //println(s"\t\texplore: $exploreCost")
      val frontierSize = frontierSizeForCandidatePattern(candidate, exploreCost, previous._1)
      if (frontierSize == 0) {
        //println("\t\tZERO value returned")
        (0, 0, 0)
      } //println(s"\t\texplore: $frontierSize")
      else
        (frontierSize, exploreCost, exploreCost)
    }

    /**
     * returns lookupcost for the candidate pattern, given the cost of previous pattern order
     */
    def exploreCostForCandidatePattern(candidate: TriplePattern, pickedPatterns: List[TriplePattern]): Double = {
      val boundVariables = pickedPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }
      val intersectionVariables = boundVariables.intersect(candidate.variableSet)

      //println(s"\t\texplorecost: candidate: $candidate already bound vars: $boundVariables, intersection: $intersectionVariables")

      //if all bound
      if ((intersectionVariables.size == candidate.variableSet.size) && candidate.p > 0) {
        //println("\t\t  everything bound")
        1
      } //if o,p bound
      else if ((candidate.o > 0 || intersectionVariables.contains(candidate.o)) && candidate.p > 0) {
        //println(s"\t\t  object bound ${edgeCounts.get(TriplePattern(0, candidate.p, 0))}")
        math.min(cardinalities(candidate), edgeCounts.get(TriplePattern(0, candidate.p, 0)))
      } //if s,p bound
      else if ((candidate.s > 0 || intersectionVariables.contains(candidate.s)) && candidate.p > 0) {
        val numberOfSubjects = 1 + (cardinalities(candidate) / edgeCounts.get(TriplePattern(0, candidate.p, 0)))
        //println(s"\t\t  subject bound card ${cardinalities(candidate)}, edgeCount ${edgeCounts.get(TriplePattern(0, candidate.p, 0))}, division ${numberOfSubjects}")
        math.min(cardinalities(candidate), numberOfSubjects)
      } else {
        //println(s"\t\t  nothing bound ${cardinalities(candidate)}")
        cardinalities(candidate)
      }
    }

    /**
     * returns msgcost for the candidate pattern, given the cost of previous pattern order and previous pattern order
     */
    def frontierSizeForCandidatePattern(candidate: TriplePattern, exploreCostOfCandidate: Double, pickedPatterns: List[TriplePattern]): Double = {
      val boundVariables = pickedPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }

      //println(s"\t\tfrontier: candidate: $candidate already bound vars: $boundVariables")

      //if either s or o is bound
      if ((candidate.o > 0 || candidate.s > 0 || boundVariables.contains(candidate.s) || boundVariables.contains(candidate.o)) && (candidate.p > 0)) {
        val minimumPredicateSelectivityCost = pickedPatterns.map { prev => calculatePredicateSelectivityCost(prev, candidate) }.min
        //println(s"\t\t  subject or object bound ${math.min(exploreCostOfCandidate, minimumPredicateSelectivityCost)}")
        math.min(exploreCostOfCandidate, minimumPredicateSelectivityCost)
      } //otherwise
      else {
        //println(s"\t\t  nothing bound ${exploreCostOfCandidate}")
        exploreCostOfCandidate
      }
    }

    def calculatePredicateSelectivityCost(prev: TriplePattern, candidate: TriplePattern): Double = {
      val upperBoundBasedOnPredicateSelectivity = (prev.s, prev.o) match {
        case (candidate.s, _) =>
          predicateSelectivity.outOut(prev.p, candidate.p)
        //calculateSelectivityCost(prev, candidate, candidate.o, predicateSelectivity.outOut(prev.p, candidate.p))
        case (candidate.o, _) =>
          predicateSelectivity.outIn(prev.p, candidate.p)
        //calculateSelectivityCost(prev, candidate, candidate.o, predicateSelectivity.outIn(prev.p, candidate.p))
        case (_, candidate.o) =>
          predicateSelectivity.inIn(prev.p, candidate.p)
        //calculateSelectivityCost(prev, candidate, candidate.s, predicateSelectivity.inIn(prev.p, candidate.p))
        case (_, candidate.s) =>
          predicateSelectivity.inOut(prev.p, candidate.p)
        //calculateSelectivityCost(prev, candidate, candidate.s, predicateSelectivity.inOut(prev.p, candidate.p))
        case other =>
          Double.MaxValue
        /*val ret =
            cardinalities(prev) * cardinalities(candidate) + orderPrevAndCandidate(prev, candidate)
          if (ret < 0) {
            Long.MaxValue
          } else {
            ret
          }*/
      }
      //println(s"\t\t  selectivity of $prev and $candidate: $upperBoundBasedOnPredicateSelectivity")
      upperBoundBasedOnPredicateSelectivity
    }

    /*
    def orderPrevAndCandidate(prev: TriplePattern, candidate: TriplePattern): Int = {
      if (cardinalities(prev) < cardinalities(candidate)) {
        -1
      } else {
        1
      }
    }

    def calculateSelectivityCost(prev: TriplePattern, candidate: TriplePattern, prevBoundOrUnbound: Int, selectivity: Long): Long = {
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
    */

    val triplePatterns = cardinalities.keys.toArray

    /**
     * find all plans whose cost have to be calculated
     * we'll store them in a lookup table
     */
    val patternWindows = for (
      size <- 2 to triplePatterns.size;
      window <- triplePatterns.combinations(size)
    ) yield window

    /**
     * create a lookup table for optimal ordering and cost for each plan
     */
    val lookupTable = patternWindows.foldLeft(Map.empty[Set[TriplePattern], Option[(List[TriplePattern], Double, Double, Double)]]) { case (result, current) => result + (current.toSet -> costOfCombination(current, result)) }

    /**
     * the optimal ordering for the query is the corresponding entry in the lookup table for the set of all patterns
     */
    val optimalCombination = lookupTable(triplePatterns.toSet).get
    val optimalOrder = {
      if (optimalCombination._4 == 0)
        List()
      else
        optimalCombination._1
    }
    //println(s"lookuptable:  ${lookupTable.mkString("\n")}")
    //println(s"optimizer: optimalorder ${lookupTable(triplePatterns.toSet).get}")
    //println(s"cardinalities: ${cardinalities.mkString(", ")}")
    println(s"optimal order: ${optimalOrder.toArray.mkString(" ")}")
    println(s"cardinalities: ${cardinalities.mkString(" ")}")
    println("edgeCounts: " + edgeCounts.mkString(" ")+"\n")
    optimalOrder.toArray
  }
}
