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

class PredicateSelectivityEdgeCountsOptimizerSpec extends FlatSpec with Checkers with TestAnnouncements {

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
      val tripleExists = tr.executeQuery(Seq(TriplePattern(s1, p1, o1)), Some(optimizer))
      val tripleNotFound = tr.executeQuery(Seq(TriplePattern(s1, p2, o1)), Some(optimizer))
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
        val queryToGetCardinality = Seq(tp)
        val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
        cardinalityQueryResult.size
      }

      //val patterns = List(TriplePattern(s1, p1, z), TriplePattern(z, p4, y), TriplePattern(y, p3, x))
      val patterns = List(TriplePattern(s1, p1, z), TriplePattern(z, p4, y))
      val cardinalities = patterns.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
      val predicateStats = Map(p1 -> PredicateStats(2l, 2l, 9l), p3 -> PredicateStats(8l, 8l, 5l), p4 -> PredicateStats(6l, 6l, 9l))
      val optimizedQuery = optimizer.optimize(cardinalities, predicateStats)

      val costMap = computePlanAndCosts(stats, predicateStats, cardinalities)
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
        val queryToGetCardinality = Seq(tp)
        val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
        cardinalityQueryResult.size
      }

      val patterns = List(TriplePattern(s1, p1, z), TriplePattern(z, p4, y), TriplePattern(y, p3, x))
      //val patterns = List(TriplePattern(s1, p1, z), TriplePattern(z, p4, y))
      val cardinalities = patterns.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
      val predicateStats = Map(p1 -> PredicateStats(2l, 2l, 9l), p3 -> PredicateStats(8l, 8l, 5l), p4 -> PredicateStats(6l, 6l, 9l))
      val optimizedQuery = optimizer.optimize(cardinalities, predicateStats)
      val costMap = computePlanAndCosts(stats, predicateStats, cardinalities)
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

  lazy val genTriplesMore = containerOfN[List, TriplePattern](100, genTriple)
  implicit lazy val arbTriples = Arbitrary(genTriplesMore map (_.toSet))
  implicit lazy val arbQuery = Arbitrary(queryPatterns)

  // TODO(Bibek): This test fails, please fix. Error output:
  //  - should correctly answer random queries with basic graph patterns *** FAILED ***
  //  NoSuchElementException was thrown during property evaluation.
  //    Message: key not found: List(TriplePattern(-1,25,-1), TriplePattern(-4,25,-1))
  //    Occurred when passed generated values (
  //      arg0 = Set(TriplePattern(6,24,23), TriplePattern(3,23,18), TriplePattern(3,24,14), TriplePattern(25,24,1), TriplePattern(7,23,25), TriplePattern(3,23,21), TriplePattern(2,25,14), TriplePattern(8,23,14), TriplePattern(1,24,2), TriplePattern(7,23,6), TriplePattern(7,24,15), TriplePattern(10,25,5), TriplePattern(9,23,7), TriplePattern(8,25,4), TriplePattern(9,24,7), TriplePattern(20,23,22), TriplePattern(23,24,9), TriplePattern(12,25,4), TriplePattern(12,24,11), TriplePattern(4,23,9), TriplePattern(17,24,8), TriplePattern(20,24,14), TriplePattern(15,25,10), TriplePattern(17,25,2), TriplePattern(6,24,8), TriplePattern(5,25,18), TriplePattern(18,23,2), TriplePattern(13,23,23), TriplePattern(2,25,20), TriplePattern(8,24,21), TriplePattern(19,23,23), TriplePattern(17,25,21), TriplePattern(3,25,16), TriplePattern(6,23,19), TriplePattern(13,25,6), TriplePattern(11,24,7), TriplePattern(3,24,6), TriplePattern(21,24,5), TriplePattern(6,24,17), TriplePattern(1,23,12), TriplePattern(2,23,11), TriplePattern(20,23,1), TriplePattern(14,23,12), TriplePattern(9,24,16), TriplePattern(1,23,3), TriplePattern(12,23,19), TriplePattern(9,23,6), TriplePattern(11,25,24), TriplePattern(9,23,16), TriplePattern(9,25,7), TriplePattern(13,23,18), TriplePattern(14,23,16), TriplePattern(3,25,2), TriplePattern(3,24,20), TriplePattern(13,25,7), TriplePattern(7,24,18), TriplePattern(24,25,7), TriplePattern(16,23,11), TriplePattern(14,25,13), TriplePattern(9,25,13), TriplePattern(16,23,9), TriplePattern(7,25,15), TriplePattern(1,25,5), TriplePattern(7,24,1), TriplePattern(12,25,17), TriplePattern(20,24,9), TriplePattern(3,24,9), TriplePattern(14,23,10), TriplePattern(7,24,21), TriplePattern(22,23,8), TriplePattern(1,23,4), TriplePattern(24,24,10), TriplePattern(14,25,17), TriplePattern(4,23,4), TriplePattern(15,24,13), TriplePattern(10,24,20), TriplePattern(10,24,9), TriplePattern(4,24,19), TriplePattern(2,24,6), TriplePattern(15,25,12), TriplePattern(11,25,15), TriplePattern(22,23,24), TriplePattern(3,24,2), TriplePattern(14,23,7), TriplePattern(18,23,12), TriplePattern(6,24,21), TriplePattern(8,23,11), TriplePattern(15,23,9), TriplePattern(22,25,3), TriplePattern(10,24,12), TriplePattern(5,25,15), TriplePattern(21,24,3), TriplePattern(7,23,5), TriplePattern(1,25,3), TriplePattern(4,25,4), TriplePattern(4,23,17), TriplePattern(9,23,23), TriplePattern(4,23,6), TriplePattern(6,23,1), TriplePattern(7,25,8), TriplePattern(12,24,2), TriplePattern(2,23,17), TriplePattern(13,24,14), TriplePattern(6,24,13), TriplePattern(9,25,17), TriplePattern(14,25,4), TriplePattern(23,25,1), TriplePattern(16,23,20), TriplePattern(6,23,3), TriplePattern(25,23,8), TriplePattern(14,23,3), TriplePattern(15,24,4), TriplePattern(8,24,5), TriplePattern(11,23,6), TriplePattern(6,23,9), TriplePattern(22,23,3), TriplePattern(22,24,11), TriplePattern(13,23,14), TriplePattern(17,23,11), TriplePattern(21,25,2), TriplePattern(5,25,14), TriplePattern(7,25,11), TriplePattern(18,25,16), TriplePattern(6,25,12), TriplePattern(15,23,13), TriplePattern(1,25,6), TriplePattern(21,24,9), TriplePattern(7,24,5), TriplePattern(17,25,22), TriplePattern(5,25,1), TriplePattern(4,24,13), TriplePattern(24,24,25), TriplePattern(3,23,17), TriplePattern(18,24,8), TriplePattern(9,25,2), TriplePattern(12,25,9), TriplePattern(20,24,7), TriplePattern(2,23,24), TriplePattern(9,24,8), TriplePattern(6,24,5), TriplePattern(13,24,22), TriplePattern(7,25,19), TriplePattern(3,24,10), TriplePattern(5,25,17), TriplePattern(8,25,11), TriplePattern(1,23,15), TriplePattern(1,23,6), TriplePattern(14,25,20), TriplePattern(7,24,6), TriplePattern(11,24,9), TriplePattern(13,25,4), TriplePattern(5,25,2), TriplePattern(17,25,13), TriplePattern(18,25,15), TriplePattern(1,24,5), TriplePattern(9,23,14), TriplePattern(20,24,20), TriplePattern(8,25,18), TriplePattern(9,25,18), TriplePattern(2,24,20), TriplePattern(5,25,3), TriplePattern(15,23,20), TriplePattern(3,23,9), TriplePattern(1,23,20), TriplePattern(4,23,5), TriplePattern(5,23,5), TriplePattern(15,25,14), TriplePattern(17,24,10), TriplePattern(11,25,2), TriplePattern(7,25,1), TriplePattern(21,24,18), TriplePattern(25,25,25), TriplePattern(9,24,15), TriplePattern(6,24,1), TriplePattern(9,24,21), TriplePattern(6,23,20), TriplePattern(9,25,3), TriplePattern(7,25,4), TriplePattern(2,24,12), TriplePattern(16,23,21), TriplePattern(4,25,6), TriplePattern(20,23,14), TriplePattern(21,24,1), TriplePattern(12,23,8), TriplePattern(8,25,3), TriplePattern(23,23,3), TriplePattern(14,24,4), TriplePattern(10,23,10), TriplePattern(5,23,23), TriplePattern(2,23,21), TriplePattern(23,23,5), TriplePattern(2,24,22), TriplePattern(8,25,1), TriplePattern(14,23,23), TriplePattern(18,23,7), TriplePattern(5,23,4), TriplePattern(3,24,3), TriplePattern(5,23,3), TriplePattern(4,24,16), TriplePattern(5,24,1), TriplePattern(5,24,12), TriplePattern(2,25,9), TriplePattern(21,23,2), TriplePattern(10,24,7), TriplePattern(24,24,9), TriplePattern(1,24,7), TriplePattern(22,25,18), TriplePattern(3,25,7), TriplePattern(6,23,18), TriplePattern(15,25,3), TriplePattern(18,24,11), TriplePattern(15,24,11), TriplePattern(22,25,14), TriplePattern(5,24,4), TriplePattern(9,24,2), TriplePattern(14,24,12), TriplePattern(9,23,8), TriplePattern(17,23,2), TriplePattern(2,24,9), TriplePattern(4,25,12), TriplePattern(20,24,11), TriplePattern(1,24,3), TriplePattern(20,25,20), TriplePattern(8,23,5), TriplePattern(25,25,23), TriplePattern(11,24,6), TriplePattern(8,25,10), TriplePattern(19,23,1), TriplePattern(7,25,7), TriplePattern(9,25,6), TriplePattern(23,23,19), TriplePattern(2,23,20), TriplePattern(12,25,24), TriplePattern(6,23,15), TriplePattern(21,25,14), TriplePattern(1,23,11), TriplePattern(18,24,4), TriplePattern(1,25,8), TriplePattern(4,24,7), TriplePattern(6,25,13), TriplePattern(13,23,1), TriplePattern(17,25,11), TriplePattern(17,23,3), TriplePattern(20,25,9), TriplePattern(10,25,12), TriplePattern(10,25,8), TriplePattern(2,24,3), TriplePattern(16,23,8), TriplePattern(7,25,2), TriplePattern(8,23,22), TriplePattern(12,25,10), TriplePattern(17,23,6), TriplePattern(9,25,4), TriplePattern(2,25,5), TriplePattern(3,23,10), TriplePattern(18,24,5), TriplePattern(4,23,15), TriplePattern(11,25,22), TriplePattern(3,23,16), TriplePattern(5,24,7), TriplePattern(6,24,7), TriplePattern(20,23,20), TriplePattern(4,24,8), TriplePattern(7,25,5), TriplePattern(7,24,13), TriplePattern(17,23,7), TriplePattern(23,23,23), TriplePattern(9,23,15), TriplePattern(8,23,23), TriplePattern(11,24,10), TriplePattern(18,25,7)),
  //      arg1 = List(TriplePattern(6,23,-1), TriplePattern(-1,25,-1), TriplePattern(-4,25,-1))
  //    )

  //  it should "correctly answer random queries with basic graph patterns" in {
  //    check(
  //      Prop.forAllNoShrink(tripleSet, queryPatterns) {
  //        (triples: Set[TriplePattern], query: List[TriplePattern]) =>
  //          val tr = new TripleRush
  //          try {
  //            for (triple <- triples) {
  //              tr.addEncodedTriple(triple.s, triple.p, triple.o)
  //            }
  //            tr.prepareExecution
  //            val stats = new PredicateSelectivity(tr)
  //            val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)
  //
  //            def calculateEdgeCountOfPattern(predicate: Int): Long = {
  //              val pIndices = triples.filter(x => x.p == predicate)
  //              val setOfSubjects = pIndices.foldLeft(Set.empty[Int]) { case (result, current) => result + current.s }
  //              setOfSubjects.size
  //            }
  //
  //            def calculateObjectCountOfPattern(predicate: Int): Long = {
  //              val pIndices = triples.filter(x => x.p == predicate)
  //              val setOfObjects = pIndices.foldLeft(Set.empty[Int]) { case (result, current) => result + current.o }
  //              setOfObjects.size
  //            }
  //
  //            def calculateSubjectCountOfPattern(predicate: Int): Long = {
  //              val pIndices = triples.filter(x => x.p == predicate)
  //              val setOfObjects = pIndices.foldLeft(Set.empty[Int]) { case (result, current) => result + current.s }
  //              setOfObjects.size
  //            }
  //
  //            def calculateCardinalityOfPattern(tp: TriplePattern): Long = {
  //              val queryToGetCardinality = QuerySpecification(List(tp))
  //              val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
  //              cardinalityQueryResult.size
  //            }
  //
  //            val cardinalities = query.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
  //            val edgeCounts = query.map(tp => (tp.p, calculateEdgeCountOfPattern(tp.p))).toMap
  //            val objectCounts = query.map(tp => (tp.p, calculateObjectCountOfPattern(tp.p))).toMap
  //            val subjectCounts = query.map(tp => (tp.p, calculateSubjectCountOfPattern(tp.p))).toMap
  //
  //            if (cardinalities.forall(_._2 > 0) && cardinalities.size > 1 && cardinalities.forall(_._1.p > 0)) {
  //              val optimizedQuery = optimizer.optimize(cardinalities, edgeCounts, objectCounts, subjectCounts)
  //              val costMap = computePlanAndCosts(stats, edgeCounts, objectCounts, subjectCounts, cardinalities)
  //              val costMapForQuery = costMap.filter(p => p._1.size == query.size).reduceLeft(minCostEstimate)
  //              val bestPatternOrderFromCostMap = costMapForQuery._1.reverse
  //              assert(optimizedQuery.toList == bestPatternOrderFromCostMap)
  //            }
  //          } finally {
  //            tr.shutdown
  //          }
  //          true
  //      }, minSuccessful(10))
  //  }

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

            val candidateFrontier = {
              //everything bound already
              if ((intersectingVariables.size == candidate.variableSet.size) && candidate.p > 0) {
                1
              } //s, p, *
              else if (isObjectBound && candidate.p > 0 && candidate.o < 0) {
                stats.objectCount
              } //*,p,o
              else if (candidate.s < 0 && candidate.p > 0 && isObjectBound) {
                stats.subjectCount
              } //s,*,o{
              else if (isSubjectBound && candidate.p < 0 && isObjectBound) {
                selectivityStats.predicates.size
              } //*,p,*
              else if (candidate.s < 0 && candidate.p > 0 && candidate.o < 0) {
                stats.edgeCount * stats.subjectCount
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