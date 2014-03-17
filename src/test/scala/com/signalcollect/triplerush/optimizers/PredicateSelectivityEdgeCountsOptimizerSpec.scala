package com.signalcollect.triplerush.optimizers

import scala.annotation.migration
import org.scalacheck.Arbitrary
import org.scalacheck.Gen.containerOfN
import org.scalatest.Finders
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.TripleGenerators.genTriple
import com.signalcollect.triplerush.TripleGenerators.queryPatterns
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.TripleGenerators._

class PredicateSelectivityEdgeCountsOptimizerSpec extends FlatSpec with Checkers {

  val s1 = 1
  val s2 = 2
  val s3 = 3
  val o1 = 101
  val o2 = 102
  val o3 = 103
  val o4 = 104
  val o5 = 105
  val o6 = 106
  val o7 = 107
  val o8 = 108
  val o9 = 109
  val o10 = 110
  val o11 = 111
  val p1 = 1001
  val p2 = 1002
  val p3 = 1003
  val p4 = 1004
  val p5 = 1005

  val x = -1
  val y = -2
  val z = -3
  val z1 = -4

  "PredicateSelectivityEdgeCountsOptimizer" should "correctly handle fully bound patterns" in {
    val tr = new TripleRush
    try {
      tr.addEncodedTriple(s1, p1, o1)
      tr.prepareExecution
      val stats = new PredicateSelectivity(tr)
      val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)
      val tripleExists = tr.executeQuery(QuerySpecification(List(TriplePattern(s1, p1, o1))), Some(optimizer))
      val tripleNotFound = tr.executeQuery(QuerySpecification(List(TriplePattern(s1, p2, o1))), Some(optimizer))
      assert(tripleExists.size == 1)
      assert(tripleNotFound.size == 0)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly find the optimal query order" in {
    val tr = new TripleRush
    try {
      tr.addEncodedTriple(s1, p1, o1)
      tr.addEncodedTriple(s1, p1, o2)
      tr.addEncodedTriple(s1, p1, o3)
      tr.addEncodedTriple(s1, p1, o4)
      tr.addEncodedTriple(s1, p1, o5)
      tr.addEncodedTriple(s1, p1, o6)
      tr.addEncodedTriple(s1, p1, o7)
      tr.addEncodedTriple(s1, p1, o8)
      tr.addEncodedTriple(s1, p1, o9)

      tr.addEncodedTriple(s1, p2, o5)
      tr.addEncodedTriple(s1, p2, o6)
      tr.addEncodedTriple(s1, p2, o7)
      tr.addEncodedTriple(s1, p2, o8)
      tr.addEncodedTriple(s1, p2, o9)

      tr.addEncodedTriple(s2, p1, o3)
      tr.addEncodedTriple(s2, p1, o4)
      tr.addEncodedTriple(s2, p1, o5)
      tr.addEncodedTriple(s2, p1, o6)

      tr.addEncodedTriple(s2, p2, o2)
      tr.addEncodedTriple(s2, p2, o3)
      tr.addEncodedTriple(s2, p2, o4)
      tr.addEncodedTriple(s2, p2, o5)
      tr.addEncodedTriple(s2, p2, o6)
      tr.addEncodedTriple(s2, p2, o7)

      tr.addEncodedTriple(s2, p3, o2)
      tr.addEncodedTriple(s2, p4, o3)
      tr.addEncodedTriple(s2, p5, o4)
      tr.addEncodedTriple(s1, p4, o6)
      tr.addEncodedTriple(s1, p4, o7)
      tr.addEncodedTriple(s1, p4, o8)
      tr.addEncodedTriple(s3, p3, o5)
      tr.addEncodedTriple(s3, p2, o10)
      tr.addEncodedTriple(s2, p3, o5)

      tr.addEncodedTriple(o5, p4, o1)
      tr.addEncodedTriple(o5, p4, o2)
      tr.addEncodedTriple(o5, p4, o3)
      tr.addEncodedTriple(o4, p4, o7)
      tr.addEncodedTriple(o4, p4, o9)
      tr.addEncodedTriple(o3, p4, o8)
      tr.addEncodedTriple(o3, p4, o9)
      tr.addEncodedTriple(o3, p4, o10)
      tr.addEncodedTriple(o2, p4, o7)

      tr.addEncodedTriple(o3, p3, o1)
      tr.addEncodedTriple(o4, p3, o1)
      tr.addEncodedTriple(o5, p3, o2)
      tr.addEncodedTriple(o9, p3, o4)
      tr.addEncodedTriple(o10, p3, o3)
      tr.addEncodedTriple(o11, p3, o4)

      tr.addEncodedTriple(o3, p5, o9)
      tr.addEncodedTriple(o10, p5, o9)

      tr.prepareExecution

      val stats = new PredicateSelectivity(tr)
      val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)

      def calculateCardinalityOfPattern(tp: TriplePattern): Long = {
        val queryToGetCardinality = QuerySpecification(List(tp))
        val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
        cardinalityQueryResult.size
      }

      //val patterns = List(TriplePattern(s1, p1, z), TriplePattern(z, p4, y), TriplePattern(y, p3, x))
      val patterns = List(TriplePattern(s1, p1, z), TriplePattern(z, p4, y))
      val cardinalities = patterns.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
      val edgeCounts = Map(p1 -> 2l, p3 -> 8l, p4 -> 6l)
      val objectCounts = Map(p1 -> 9l, p3 -> 5l, p4 -> 9l)
      val subjectCounts = Map(p1 -> 2l, p3 -> 8l, p4 -> 6l)

      val optimizedQuery = optimizer.optimize(cardinalities, edgeCounts, objectCounts, subjectCounts)

      val costMap = computePlanAndCosts(stats, edgeCounts, objectCounts, subjectCounts, cardinalities)
      val costMapForQuery = costMap.filter(p => p._1.size == patterns.size).reduceLeft(minCostEstimate)
      val bestPatternOrderFromCostMap = costMapForQuery._1.reverse

      assert(optimizedQuery.toList == bestPatternOrderFromCostMap)
    } finally {
      tr.shutdown
    }
  }

  def minCostEstimate(cost1: (List[TriplePattern], CostEstimate), cost2: (List[TriplePattern], CostEstimate)): (List[TriplePattern], CostEstimate) = if (cost1._2.explorationSum < cost2._2.explorationSum) cost1 else cost2

  case class CostEstimate(frontier: Double, lastExploration: Double, explorationSum: Double)

  it should "match the optimal query order" in {
    val tr = new TripleRush
    try {
      tr.addEncodedTriple(s1, p1, o1)
      tr.addEncodedTriple(s1, p1, o2)
      tr.addEncodedTriple(s1, p1, o3)
      tr.addEncodedTriple(s1, p1, o4)
      tr.addEncodedTriple(s1, p1, o5)
      tr.addEncodedTriple(s1, p1, o6)
      tr.addEncodedTriple(s1, p1, o7)
      tr.addEncodedTriple(s1, p1, o8)
      tr.addEncodedTriple(s1, p1, o9)

      tr.addEncodedTriple(s1, p2, o5)
      tr.addEncodedTriple(s1, p2, o6)
      tr.addEncodedTriple(s1, p2, o7)
      tr.addEncodedTriple(s1, p2, o8)
      tr.addEncodedTriple(s1, p2, o9)

      tr.addEncodedTriple(s2, p1, o3)
      tr.addEncodedTriple(s2, p1, o4)
      tr.addEncodedTriple(s2, p1, o5)
      tr.addEncodedTriple(s2, p1, o6)

      tr.addEncodedTriple(s2, p2, o2)
      tr.addEncodedTriple(s2, p2, o3)
      tr.addEncodedTriple(s2, p2, o4)
      tr.addEncodedTriple(s2, p2, o5)
      tr.addEncodedTriple(s2, p2, o6)
      tr.addEncodedTriple(s2, p2, o7)

      tr.addEncodedTriple(s2, p3, o2)
      tr.addEncodedTriple(s2, p4, o3)
      tr.addEncodedTriple(s2, p5, o4)
      tr.addEncodedTriple(s1, p4, o6)
      tr.addEncodedTriple(s1, p4, o7)
      tr.addEncodedTriple(s1, p4, o8)
      tr.addEncodedTriple(s3, p3, o5)
      tr.addEncodedTriple(s3, p2, o10)
      tr.addEncodedTriple(s2, p3, o5)

      tr.addEncodedTriple(o5, p4, o1)
      tr.addEncodedTriple(o5, p4, o2)
      tr.addEncodedTriple(o5, p4, o3)
      tr.addEncodedTriple(o4, p4, o7)
      tr.addEncodedTriple(o4, p4, o9)
      tr.addEncodedTriple(o3, p4, o8)
      tr.addEncodedTriple(o3, p4, o9)
      tr.addEncodedTriple(o3, p4, o10)
      tr.addEncodedTriple(o2, p4, o7)

      tr.addEncodedTriple(o3, p3, o1)
      tr.addEncodedTriple(o4, p3, o1)
      tr.addEncodedTriple(o5, p3, o2)
      tr.addEncodedTriple(o9, p3, o4)
      tr.addEncodedTriple(o10, p3, o3)
      tr.addEncodedTriple(o11, p3, o4)

      tr.addEncodedTriple(o3, p5, o9)
      tr.addEncodedTriple(o10, p5, o9)

      tr.prepareExecution
      val stats = new PredicateSelectivity(tr)
      val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)

      def calculateCardinalityOfPattern(tp: TriplePattern): Long = {
        val queryToGetCardinality = QuerySpecification(List(tp))
        val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
        cardinalityQueryResult.size
      }

      val patterns = List(TriplePattern(s1, p1, z), TriplePattern(z, p4, y), TriplePattern(y, p3, x))
      //val patterns = List(TriplePattern(s1, p1, z), TriplePattern(z, p4, y))
      val cardinalities = patterns.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
      val edgeCounts = Map(p1 -> 2l, p3 -> 8l, p4 -> 6l)
      val objectCounts = Map(p1 -> 9l, p3 -> 5l, p4 -> 9l)
      val subjectCounts = Map(p1 -> 2l, p3 -> 8l, p4 -> 6l)

      val optimizedQuery = optimizer.optimize(cardinalities, edgeCounts, objectCounts, subjectCounts)
      val costMap = computePlanAndCosts(stats, edgeCounts, objectCounts, subjectCounts, cardinalities)
      val costMapForQuery = costMap.filter(p => p._1.size == patterns.size).reduceLeft(minCostEstimate)
      //println(s"costMap: $costMapForQuery")
      val bestPatternOrderFromCostMap = costMapForQuery._1.reverse

      //println(s"optimizer returned: ${optimizedQuery.toList}, we found: $bestPatternOrderFromCostMap")
      assert(optimizedQuery.toList == bestPatternOrderFromCostMap)
      //assert(optimizedQuery.toList == List(TriplePattern(x, p2, y), TriplePattern(y, p4, z)))
    } finally {
      tr.shutdown
    }
  }

  lazy val genTriplesMore = containerOfN[List, TriplePattern](500, genTriple)
  implicit lazy val arbTriples = Arbitrary(genTriplesMore map (_.toSet))
  implicit lazy val arbQuery = Arbitrary(queryPatterns)
  //implicit lazy val genQueries = containerOfN[List, TriplePattern](10, genQueryPattern)
  //implicit lazy val arbQuery = Arbitrary(genQueries)

  it should "correctly answer random queries with basic graph patterns" in {
    check((triples: Set[TriplePattern], queries: List[TriplePattern]) => {
      val tr = new TripleRush
      try {
        for (triple <- triples) {
          tr.addEncodedTriple(triple.s, triple.p, triple.o)
        }
        tr.prepareExecution
        val stats = new PredicateSelectivity(tr)
        val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)

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

        def calculateCardinalityOfPattern(tp: TriplePattern): Long = {
          val queryToGetCardinality = QuerySpecification(List(tp))
          val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
          cardinalityQueryResult.size
        }

        val cardinalities = queries.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
        val edgeCounts = queries.map(tp => (tp.p, calculateEdgeCountOfPattern(tp.p))).toMap
        val objectCounts = queries.map(tp => (tp.p, calculateObjectCountOfPattern(tp.p))).toMap
        val subjectCounts = queries.map(tp => (tp.p, calculateSubjectCountOfPattern(tp.p))).toMap

        if (cardinalities.forall(_._2 > 0) && cardinalities.size > 1 && cardinalities.forall(_._1.p > 0)) {
          val optimizedQuery = optimizer.optimize(cardinalities, edgeCounts, objectCounts, subjectCounts)
          val costMap = computePlanAndCosts(stats, edgeCounts, objectCounts, subjectCounts, cardinalities)
          val costMapForQuery = costMap.filter(p => p._1.size == queries.size).reduceLeft(minCostEstimate)
          val bestPatternOrderFromCostMap = costMapForQuery._1.reverse
          assert(optimizedQuery.toList == bestPatternOrderFromCostMap)
        }
      } finally {
        tr.shutdown
      }
      true
    }, minSuccessful(3))
  }

  def computePlanAndCosts(
    selectivityStats: PredicateSelectivity,
    edgeCounts: Map[Int, Long],
    objectCounts: Map[Int, Long],
    subjectCounts: Map[Int, Long],
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

            val candidateFrontier = {
              //everything bound already
              if ((intersectingVariables.size == candidate.variableSet.size) && candidate.p > 0) {
                1
              } //s, p, *
              else if (isObjectBound && candidate.p > 0 && candidate.o < 0) {
                objectCounts(pIndex)
              } //*,p,o
              else if (candidate.s < 0 && candidate.p > 0 && isObjectBound) {
                subjectCounts(pIndex)
              } //s,*,o{
              else if (isSubjectBound && candidate.p < 0 && isObjectBound) {
                selectivityStats.predicates.size
              } //*,p,*
              else if (candidate.s < 0 && candidate.p > 0 && candidate.o < 0) {
                edgeCounts(pIndex) * subjectCounts(pIndex)
              } else {
                cardinalities(candidate)
              }
            }

            val exploreCost = costMap(previous).frontier * (math.min(candidateFrontier, cardinalities(candidate)))

            //calculating frontier size
            val frontierSize = {
              if ((candidate.o > 0 || candidate.s > 0 || boundVariables.contains(candidate.s) || boundVariables.contains(candidate.o)) && (candidate.p > 0)) {
                val minPredicateSelectivityCost = previous.map { prev => calculatePredicateSelectivityCost(prev, candidate, selectivityStats) }.min
                math.min(exploreCost, minPredicateSelectivityCost)
              } //otherwise
              else {
                exploreCost
              }
            }

            val lastExploreCost = costMap(previous).explorationSum
            if (frontierSize == 0) {
              costMap += List() -> CostEstimate(0, 0, 0)
            } else {
              costMap += permutation -> CostEstimate(frontierSize, exploreCost, exploreCost + lastExploreCost)
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