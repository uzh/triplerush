package com.signalcollect.triplerush.optimizers

import scala.annotation.migration
import org.scalacheck.Arbitrary
import org.scalacheck.Gen.containerOfN
import org.scalatest.Finders
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TripleGenerators.genTriple
import com.signalcollect.triplerush.TripleGenerators.queryPatterns
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.TripleGenerators._
import com.signalcollect.triplerush.TestAnnouncements
import org.scalacheck.Prop
import com.signalcollect.triplerush.PredicateStats
import com.signalcollect.triplerush.PredicateStats

case class CostEstimate(frontier: Double, lastExploration: Double, explorationSum: Double)

class HeuristicOptimizerSpec extends FlatSpec with Checkers with TestAnnouncements {

  def minCostEstimate(cost1: (List[TriplePattern], CostEstimate), cost2: (List[TriplePattern], CostEstimate)): (List[TriplePattern], CostEstimate) = if (cost1._2.explorationSum < cost2._2.explorationSum) cost1 else cost2

  lazy val genTriplesMore = containerOfN[List, TriplePattern](100, genTriple)
  implicit lazy val arbTriples = Arbitrary(genTriplesMore map (_.toSet))
  implicit lazy val arbQuery = Arbitrary(queryPatterns)

  "HeuristicsOptimizer" should "correctly answer random queries with basic graph patterns" in {
    check(
      Prop.forAllNoShrink(tripleSet, queryPatterns) {
        (triples: Set[TriplePattern], query: List[TriplePattern]) =>
          val tr = new TripleRush
          try {
            for (triple <- triples) {
              tr.addEncodedTriple(triple.s, triple.p, triple.o)
            }
            tr.prepareExecution
            val stats = new PredicateSelectivity(tr)
            val heuristicsOptimizer = new ExplorationHeuristicsOptimizer(stats)
            val explorationOptimizer = new ExplorationOptimizer(stats)
            val predicateSelectivityOptimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)

            def calculateCardinalityOfPattern(tp: TriplePattern): Long = {
              val queryToGetCardinality = Seq(tp)
              val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
              cardinalityQueryResult.size
            }

            def calculateEdgeCountOfPattern(predicate: Int): Long = {
              val pIndices = triples.filter(x => x.p == predicate)
              val setOfSubjects = pIndices.foldLeft(Set.empty[Int]) { case (result, current) => result + current.s }
              setOfSubjects.size
            }

            def calculateObjectCountOfPattern(predicate: Int): Long = {
              val pIndices = triples.filter(x => x.p == predicate)
              val setOfObjects = pIndices.foldLeft(Set.empty[Int]) { case (result, current) => result + current.o }
              setOfObjects.size
            }

            def calculateSubjectCountOfPattern(predicate: Int): Long = {
              val pIndices = triples.filter(x => x.p == predicate)
              val setOfObjects = pIndices.foldLeft(Set.empty[Int]) { case (result, current) => result + current.s }
              setOfObjects.size
            }

            val cardinalities = query.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
            val edgeCounts = query.map(tp => (tp.p, calculateEdgeCountOfPattern(tp.p))).toMap
            val objectCounts = query.map(tp => (tp.p, calculateObjectCountOfPattern(tp.p))).toMap
            val subjectCounts = query.map(tp => (tp.p, calculateSubjectCountOfPattern(tp.p))).toMap

            val predicateStats = query.map(tp => (tp.p, PredicateStats(edgeCounts(tp.p), subjectCounts(tp.p), objectCounts(tp.p)))).toMap

            if (cardinalities.forall(_._2 > 0) && cardinalities.size > 1 && cardinalities.forall(_._1.p > 0)) {

              //IMPORTANT: don't forget to reverse these query-orders before looking up their cost in costMap !!
              val explorationOptimizedQuery = explorationOptimizer.optimize(cardinalities, predicateStats)
              val heuristicsOptimizedQuery = heuristicsOptimizer.optimize(cardinalities, predicateStats)
              assert(explorationOptimizedQuery.toList == heuristicsOptimizedQuery.toList)
              /*val predicateSelectivityOptimizedQuery = predicateSelectivityOptimizer.optimize(cardinalities, predicateStats)
              val costMap = computePlanAndCosts(stats, predicateStats, cardinalities)

              val filteredCostMapForQuery = costMap.filter(p => p._1.size == query.size)
              if (!filteredCostMapForQuery.isEmpty) {
                val costMapForQuery = filteredCostMapForQuery.reduceLeft(minCostEstimate)
                val bestPatternOrderFromCostMap = costMapForQuery._1
                //println(s"cost of best query: ${costMapForQuery._2.explorationSum}, predsel: ${costMap(predicateSelectivityOptimizedQuery.toList).explorationSum}, exploration: ${costMap(explorationOptimizedQuery.toList).explorationSum}, heuristic: ${costMap(heuristicsOptimizedQuery.toList).explorationSum}")
                assert(explorationOptimizedQuery.toList == heuristicsOptimizedQuery.toList)
                /*if (costMap(predicateSelectivityOptimizedQuery.toList).explorationSum > costMapForQuery._2.explorationSum) {
                  print("FLAG: PredSel has problem")
                  println(s"cardinalities: ${cardinalities.mkString(", ")}")
                  println(s"predstats: ${predicateStats.mkString(", ")}")
                  println(s"optimized query: ${predicateSelectivityOptimizedQuery.toList.mkString(", ")},\nbest query: ${bestPatternOrderFromCostMap.mkString(", ")}\n${filteredCostMapForQuery.mkString(", ")}")
                }
                if (costMap(heuristicsOptimizedQuery.toList).explorationSum > costMap(predicateSelectivityOptimizedQuery.toList).explorationSum)
                  print("FLAG: Heuristics has problem")
                if (costMap(predicateSelectivityOptimizedQuery.toList).explorationSum < costMapForQuery._2.explorationSum)
                  print(s"STRANGE: predsel optimized query: ${predicateSelectivityOptimizedQuery.toList.mkString(", ")},\nbest query: ${bestPatternOrderFromCostMap.mkString(", ")}\n${filteredCostMapForQuery.mkString(", ")}")
                if (costMap(heuristicsOptimizedQuery.toList).explorationSum < costMapForQuery._2.explorationSum)
                  print(s"STRANGE: heuristic optimized query: ${heuristicsOptimizedQuery.toList.mkString(", ")},\nbest query: ${bestPatternOrderFromCostMap.mkString(", ")}\n${filteredCostMapForQuery.mkString(", ")}")
                if (heuristicsOptimizedQuery.toList != explorationOptimizedQuery.toList)
                  print(s"STRANGE: heuristic != exploration optimized query: ${heuristicsOptimizedQuery.toList.mkString(", ")},\nbest query: ${explorationOptimizedQuery.toList.mkString(", ")}\n${filteredCostMapForQuery.mkString(", ")}")
                  * */
                println("success")
              }*/
            }
          } finally {
            tr.shutdown
          }
          true
      }, minSuccessful(20))
  }

  def computePlanAndCosts(
    selectivityStats: PredicateSelectivity,
    predicateStats: Map[Int, PredicateStats],
    cardinalities: Map[TriplePattern, Long]): Map[List[TriplePattern], CostEstimate] = {
    var costMap = Map[List[TriplePattern], CostEstimate]()
    for (i <- 1 to cardinalities.size) {
      val plans = cardinalities.keys.toList.combinations(i)

      for (combination <- plans) {
        val planPermutations = combination.permutations.toList
        for (permutation <- planPermutations) {
          if (i == 1) {
            val pattern = permutation(0)
            costMap += permutation -> CostEstimate(cardinalities(pattern), cardinalities(pattern), cardinalities(pattern))
          } else {
            val candidate = permutation.head
            val previous = permutation.tail
            val boundVariables = previous.foldLeft(Set.empty[Int]) { case (result, current) => result.union(current.variableSet) }

            //calculating exploration cost
            val intersectingVariables = boundVariables.intersect(candidate.variableSet)
            def isSubjectBound = (candidate.s > 0 || intersectingVariables.contains(candidate.s))
            def isObjectBound = (candidate.o > 0 || intersectingVariables.contains(candidate.o))
            val pIndex = candidate.p
            val stats = predicateStats(pIndex)
            val numberOfPredicates = selectivityStats.predicates.size

            val exploreCost: Double = {
              if (candidate.p > 0) {
                val stats = predicateStats(pIndex)
                //if all bound
                if ((intersectingVariables.size == candidate.variableSet.size)) {
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

            //val exploreCost = costMap(previous).frontier * (math.min(candidateFrontier, cardinalities(candidate)))
            //val newExploreCost = lastExploreCost + exploreCost
            val newExploreCost = costMap(previous).frontier * exploreCost

            //calculating frontier size
            val frontierSize = {
              if (previous.isEmpty) {
                newExploreCost
              } //if either s or o is bound)
              else if ((candidate.o > 0 || candidate.s > 0 || boundVariables.contains(candidate.s) || boundVariables.contains(candidate.o)) && (candidate.p > 0)) {
                //TODO: make minimum computation more efficient
                val minimumPredicateSelectivityCost = {
                  previous.map { prev => if (prev.p < 0) Double.MaxValue else calculatePredicateSelectivityCost(prev, candidate, selectivityStats) }.min
                }
                math.min(newExploreCost, minimumPredicateSelectivityCost)
              } //otherwise
              else {
                newExploreCost
              }
            }

            val lastExploreCost = costMap(previous).explorationSum
            if (frontierSize == 0) {
              costMap += List() -> CostEstimate(0, 0, 0)
            } else {
              costMap += permutation -> CostEstimate(frontierSize, newExploreCost, newExploreCost + lastExploreCost)
            }

          }
        }
      }
    }
    costMap
  }

  def calculatePredicateSelectivityCost(prev: TriplePattern, candidate: TriplePattern, selectivityStats: PredicateSelectivity): Double = {
    val upperBoundBasedOnPredicateSelectivity = (prev.s, prev.o) match {
      case (candidate.s, _) =>
        selectivityStats.outOut(prev.p, candidate.p)
      case (candidate.o, _) =>
        selectivityStats.outIn(prev.p, candidate.p)
      case (_, candidate.o) =>
        selectivityStats.inIn(prev.p, candidate.p)
      case (_, candidate.s) =>
        selectivityStats.inOut(prev.p, candidate.p)
      case other =>
        Double.MaxValue
    }
    upperBoundBasedOnPredicateSelectivity
  }

}