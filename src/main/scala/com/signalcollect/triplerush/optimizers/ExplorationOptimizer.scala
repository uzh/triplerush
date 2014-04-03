package com.signalcollect.triplerush.optimizers
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import scala.annotation.tailrec
import com.signalcollect.triplerush.PredicateStats

final class ExplorationOptimizer(predicateSelectivity: PredicateSelectivity, reliableStats: Boolean = true) extends Optimizer {

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
          math.min(cardinalities(candidate), stats.edgeCount * stats.subjectCount)
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
          pickedPatterns.map { prev => if (prev.p < 0) Double.MaxValue else calculatePredicateSelectivityCost(prev, candidate) }.min
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

    val allPatterns = cardinalities.keySet

    val sizeOfFullPlan = cardinalities.size

    val planHeap = new QueryPlanMinHeap(100 * sizeOfFullPlan * sizeOfFullPlan)

    triplePatterns.foreach { tp =>
      val cardinality = cardinalities(tp).toDouble
      val atomicPlan = QueryPlan(id = Set(tp), cost = cardinality, patternOrdering = List(tp), fringe = cardinality)
      planHeap.insert(atomicPlan)
    }

    def extend(p: QueryPlan, tp: TriplePattern): QueryPlan = {
      val exploreCost = exploreCostForCandidatePattern(tp, p.patternOrdering)
      val totalCost = p.cost + exploreCost
      val fringeAfterExploration = frontierSizeForCandidatePattern(tp, exploreCost, p.patternOrdering)
      QueryPlan(id = p.id + tp, cost = totalCost, patternOrdering = tp :: p.patternOrdering, fringe = fringeAfterExploration)
    }

    var goodCompletePlan: QueryPlan = null
    while (goodCompletePlan == null) {
      val topPlan = planHeap.remove
      if (reliableStats && topPlan.fringe == 0) {
        return Array()
      }
      if (topPlan.id.size == sizeOfFullPlan) {
        goodCompletePlan = topPlan
      } else {
        val candidatePatterns = allPatterns -- topPlan.id
        candidatePatterns.foreach { tp =>
          val extendedPlan = extend(topPlan, tp)
          planHeap.insert(extendedPlan)
        }
      }
    }

    val resultOrder = goodCompletePlan.patternOrdering.toArray
    reverseMutableArray(resultOrder)
    resultOrder
  }
}
