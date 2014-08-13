package com.signalcollect.triplerush.optimizers

import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import scala.annotation.tailrec
import com.signalcollect.triplerush.PredicateStats
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.triplerush.Dictionary
import scala.collection.immutable.Map
import com.signalcollect.triplerush.TriplePattern

final class ExplorationHeuristicsOptimizer(
  val predicateSelectivity: PredicateSelectivity,
  val reliableStats: Boolean = true,
  val useHeuristic: Boolean = false) extends Optimizer {

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

    /**
     * parameters:
     * 		list of candidate pattern (length 1 or 2)
     *   	Optional list of previous optimal pattern order and the corresponding costs: size of frontier, exploration cost, totalcost
     * returns:
     * 		optimal ordering of candidate with previously picked patterns, together with the corresponding costs: size of frontier, exploration cost, totalcost
     */
    /*def costOfPatternGivenPrevious(candidate: TriplePattern, previous: (List[TriplePattern], CostEstimate)): (List[TriplePattern], CostEstimate) = {
      val cost: (List[TriplePattern], CostEstimate) = {
        val res = costForPattern(candidate, previous)
        if (res.lastExploration == 0) {
          (candidate :: previous._1, CostEstimate(0, 0, 0))
        } else {
          (candidate :: previous._1, CostEstimate(res.frontier, res.lastExploration, res.explorationSum + previous._2.explorationSum))
        }
      }
      cost
    }*/

    /**
     * parameters:
     * 		candidate pattern
     *   	previously picked pattern order with the corresponding costs: size of frontier, exploration cost, totalcost
     * returns:
     * 		cost of the order: size of frontier, exploration cost, totalcost
     */
    /*def costForPattern(candidate: TriplePattern, previous: (List[TriplePattern], CostEstimate)): CostEstimate = {
      val exploreCost = previous._2.frontier * exploreCostForCandidatePattern(candidate, previous._1)
      val frontierSize = frontierSizeForCandidatePattern(candidate, exploreCost, previous._1)
      if (frontierSize == 0) {
        CostEstimate(0, 0, 0)
      } else {
        CostEstimate(frontierSize, exploreCost, exploreCost)
      }
    }*/

    /**
     * returns lookupcost for the candidate pattern, given the cost of previous pattern order
     */
    def exploreCostForCandidatePattern(candidate: TriplePattern, pickedPatterns: List[TriplePattern], boundVariables: Set[Int]): Double = {
      //val boundVariables = pickedPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }
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
    def frontierSizeForCandidatePattern(candidate: TriplePattern, exploreCostOfCandidate: Double, pickedPatterns: List[TriplePattern], boundVariables: Set[Int]): Double = {
      //val boundVariables = pickedPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }

      if (pickedPatterns.isEmpty) {
        exploreCostOfCandidate
      } //if either s or o is bound)
      else if ((candidate.o > 0 || candidate.s > 0 || boundVariables.contains(candidate.s) || boundVariables.contains(candidate.o)) && (candidate.p > 0)) {
        //TODO: make minimum computation more efficient
        var minFrontierSizeEstimate = exploreCostOfCandidate // assume the worst.
        pickedPatterns.foreach {
          pattern =>
            if (pattern.p > 0) {
              minFrontierSizeEstimate = math.min(minFrontierSizeEstimate, calculatePredicateSelectivityCost(pattern, candidate))
            }
        }
        minFrontierSizeEstimate
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
    val sizeOfFullPlan = cardinalities.size
    var sizeOfEasilyBindablePatterns = 0
    def analyzeQuery(): Map[Int, ArrayBuffer[TriplePattern]] = {

      //map from variable (vertex) to set of associated patterns (possibly empty)
      var starMap = Map.empty[Int, Set[TriplePattern]].withDefaultValue(Set.empty)
      triplePatterns.map {
        //add patterns associated with each subject and object (vertex) to the map
        tp =>
          var tpSet = starMap(tp.s)
          starMap += tp.s -> (tpSet + tp)
          tpSet = starMap(tp.o)
          starMap += tp.o -> (tpSet + tp)
      }

      //val sortedStarMap = starMap.filter(_._2.size > 1).toArray.sortBy(_._2.size).reverse
      //val keys = starMap.filter(_._2.size > 1).map(x => x._1 -> x._2.size)

      //val rearrangedStarMap = sortedStarMap.map {

      //go through starMap to find the set of easily bindable patterns for each vertex
      //if there's a 1-1 relation between the subject and object of the pattern, it is easily bindable
      val rearrangedStarMap = starMap.map {
        case (vertex, patterns) =>
          val rearrangedStar = ArrayBuffer[TriplePattern]()
          patterns.foreach {
            tp =>
              val stats = predicateStats(tp.p)
              if ((tp.s > 0 && stats.objectCount == 1) || (tp.o > 0 && stats.subjectCount == 1)) {
                rearrangedStar.append(tp)
                sizeOfEasilyBindablePatterns += 1
              }
            //else if (!keys.contains(otherEnd) || keys(otherEnd) > patterns.size)
            //rearrangedStar.append(tp)
          }
          (vertex, rearrangedStar)
      }

      rearrangedStarMap
    }

    val easilyBindablePatterns = analyzeQuery
    //val sizeOfEasilyBindablePatterns = easilyBindablePatterns.foldLeft(0)(_ + _._2.size)
    //println(s"cardinalities: ${cardinalities.mkString(", ")}")
    //println(s"easily bindable patterns: ${easilyBindablePatterns.mkString(", ")}, size: $sizeOfEasilyBindablePatterns")

    //if there are no easily bindable patterns, simply use the exploration optimizer
    if (sizeOfEasilyBindablePatterns == 0) {
      val alternativeOptimizer = new ExplorationOptimizer(predicateSelectivity, useHeuristic = false)
      return alternativeOptimizer.optimize(cardinalities, predicateStats)
    }

    def extend(p: QueryPlan, tp: TriplePattern, boundVars: Set[Int]): QueryPlan = {
      val easyToBindPatterns = boundVars.foldLeft(Set.empty[TriplePattern]) { case (result, current) => result.union(easilyBindablePatterns(current).toSet) } -- p.patternOrdering
      val costOfEasyBinding = p.fringe * easyToBindPatterns.size
      //println(s"extend: easyToBindPatterns: ${easyToBindPatterns}, costOfEasyBinding: $costOfEasyBinding")

      val newPatternOrdering = p.patternOrdering ::: easyToBindPatterns.toList
      //val newPatternOrdering = easyToBindPatterns.toList ::: p.patternOrdering

      val boundVariables = newPatternOrdering.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }
      
      //val exploreCost = exploreCostForCandidatePattern(tp, p.patternOrdering)
      val exploreCost = exploreCostForCandidatePattern(tp, newPatternOrdering, boundVars)
      //val newCostSoFar = p.costSoFar + exploreCost
      val newCostSoFar = p.costSoFar + costOfEasyBinding + exploreCost
      //val matchedPatternsSoFar = p.id.size + 1
      val matchedPatternsSoFar = newPatternOrdering.size + 1
      val remainingUnmatchedPatterns = sizeOfFullPlan - matchedPatternsSoFar //TODO: change this?
      val newEstimatedTotalCost = if (useHeuristic) {
        val avgPatternCostSoFar = newCostSoFar / matchedPatternsSoFar
        newCostSoFar + remainingUnmatchedPatterns * avgPatternCostSoFar
      } else {
        newCostSoFar
      }
      //val fringeAfterExploration = frontierSizeForCandidatePattern(tp, exploreCost, p.patternOrdering)
      val fringeAfterExploration = frontierSizeForCandidatePattern(tp, exploreCost, newPatternOrdering, boundVars)
      QueryPlan(
        //id = p.id + tp,
        id = newPatternOrdering.toSet + tp,
        costSoFar = newCostSoFar,
        estimatedTotalCost = newEstimatedTotalCost,
        patternOrdering = tp :: newPatternOrdering,
        fringe = fringeAfterExploration)
    }

    val allPatterns = cardinalities.keySet
    val planHeap = new QueryPlanMinHeap(100 * sizeOfFullPlan * sizeOfFullPlan)

    allPatterns.foreach { tp =>
      val cardinality = cardinalities(tp).toDouble
      val atomicPlan = QueryPlan(
        id = Set(tp),
        costSoFar = cardinality,
        estimatedTotalCost = cardinality * sizeOfFullPlan, //TODO: change?
        patternOrdering = List(tp),
        fringe = cardinality)
      planHeap.insert(atomicPlan)

      //val easilyBindable = Set(tp.s, tp.o).foldLeft(Set.empty[TriplePattern]) { case (result, current) => result.union(easilyBindablePatterns(current).toSet) }
      val easilyBindableForThisPattern = easilyBindablePatterns(tp.s) ++ easilyBindablePatterns(tp.o)
      if (easilyBindableForThisPattern.size > 0) {
        easilyBindableForThisPattern.append(tp)
        val sizeOfPatternsSoFar = easilyBindableForThisPattern.size + 1
        val costSoFar = cardinality + cardinality * (easilyBindableForThisPattern.size)
        val estimatedCost = costSoFar + (sizeOfFullPlan - sizeOfPatternsSoFar) * costSoFar / sizeOfPatternsSoFar

        val atomicPlanWithEasyBindable = QueryPlan(
          id = easilyBindableForThisPattern.toSet,
          costSoFar = costSoFar,
          estimatedTotalCost = estimatedCost,

          patternOrdering = easilyBindableForThisPattern.toList,
          //patternOrdering = easilyBindable.toList ::: List(tp),
          //patternOrdering = tp :: easilyBindable.toList,
          fringe = costSoFar)
        planHeap.insert(atomicPlanWithEasyBindable)
        //println(s"inserted ${atomicPlanWithEasyBindable}, to planheap")
      }
    }

    var goodCompletePlan: QueryPlan = null

    var numberOfSteps = 0
    var numberOfExtends = 0

    while (goodCompletePlan == null) {
      numberOfSteps += 1
      if (planHeap.isEmpty) {
        println(s"planHeap isEmpty")
      }
      val topPlan = planHeap.remove
      if (reliableStats && topPlan.fringe == 0) {
        return Array()
      }
      if (topPlan.id.size == sizeOfFullPlan) {
        //if (topPlan.id.size == sizeOfNonStarPatterns) {
        goodCompletePlan = topPlan
      } else {
        var timesIntersected = 0
        val candidatePatterns = allPatterns -- topPlan.id
        //val candidatePatterns = nonStarPatterns -- topPlan.id
        val boundVariables = topPlan.patternOrdering.foldLeft(Set.empty[Int]) { case (result, current) => result.union(Set(current.s, current.o)) }
        candidatePatterns.foreach { tp =>
          val numberOfCommonVariables = boundVariables.intersect(Set(tp.s, tp.o)).size
          if (numberOfCommonVariables > 0) {
            timesIntersected += 1
            val extendedPlan = extend(topPlan, tp, boundVariables)
            planHeap.insert(extendedPlan)
            //println(s"extendedPlan: $extendedPlan")
            numberOfExtends += 1
          }
        }
        if (timesIntersected == 0) {
          println(s"No Intersection Found, topPlan: ${topPlan.toString()}, candidate: ${candidatePatterns.mkString(", ")}")
          //TODO: How to deal with this situation? throw an exception?
        }
      }
    }

    //var starLiterals = starPatterns.foldLeft(Set.empty[Int]) { case (result, current) => result.union(Set(current.s, current.o).filter(_ > 0)) }
    //println(s"Number of steps: $numberOfSteps, number of extends: $numberOfExtends")

    val resultOrder = goodCompletePlan.patternOrdering.toArray
    reverseMutableArray(resultOrder)
    //println(s"optimal order: ${resultOrder.mkString(", ")}")
    resultOrder

  }
}

