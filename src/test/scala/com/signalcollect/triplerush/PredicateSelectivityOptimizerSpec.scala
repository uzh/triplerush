package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

class PredicateSelectivityOptimizerSpec extends FlatSpec with Checkers {

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

  "PredicateSelectivityOptimizer" should "order the patterns in queries " in {
    val tr = new TripleRush
    tr.addEncodedTriple(s1, p1, o1)
    tr.addEncodedTriple(s2, p1, o2)
    tr.addEncodedTriple(s1, p2, o3)
    tr.addEncodedTriple(s1, p2, o4)
    tr.addEncodedTriple(s3, p2, o10)
    tr.addEncodedTriple(s2, p3, o5)
    tr.addEncodedTriple(o5, p4, o6)
    tr.addEncodedTriple(o4, p4, o7)
    tr.addEncodedTriple(o3, p4, o8)
    tr.addEncodedTriple(o10, p4, o11)
    tr.addEncodedTriple(o3, p5, o9)
    tr.addEncodedTriple(o10, p5, o9)
    tr.prepareExecution

    def calculateCardinalityOfPattern(tp: TriplePattern): Int = {
      val queryToGetCardinality = QuerySpecification(List(tp))
      val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality.toParticle)
      cardinalityQueryResult.size
    }

    val stats = new PredicateSelectivity(tr)
    val optimizer = new PredicateSelectivityOptimizer
    val statsForOptimizer = PredPairAndBranchingStatistics(stats.mapOutOut, stats.mapInOut, stats.mapInIn, stats.mapOutIn, stats.mapPredicateBranching)

    val patterns = List(TriplePattern(y, p4, z), TriplePattern(x, p2, y))
    val cardinalities = patterns.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
    val queryWithCardinalities = OptimizableQueryWithStats(patterns, cardinalities)
    val optimizedQuery = optimizer.optimize(queryWithCardinalities, statsForOptimizer)
    assert(optimizedQuery == List(TriplePattern(x, p2, y), TriplePattern(y, p4, z)))
  }

  "PredicateSelectivityOptimizer" should "order the patterns in another query " in {
    val tr = new TripleRush
    tr.addEncodedTriple(s1, p1, o1)
    tr.addEncodedTriple(s2, p1, o2)
    tr.addEncodedTriple(s1, p2, o3)
    tr.addEncodedTriple(s1, p2, o4)
    tr.addEncodedTriple(s3, p2, o10)
    tr.addEncodedTriple(s2, p3, o5)
    tr.addEncodedTriple(o5, p4, o6)
    tr.addEncodedTriple(o4, p4, o7)
    tr.addEncodedTriple(o3, p4, o8)
    tr.addEncodedTriple(o10, p4, o11)
    tr.addEncodedTriple(o3, p5, o9)
    tr.addEncodedTriple(o10, p5, o9)
    tr.prepareExecution

    def calculateCardinalityOfPattern(tp: TriplePattern): Int = {
      val queryToGetCardinality = QuerySpecification(List(tp))
      val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality.toParticle)
      cardinalityQueryResult.size
    }

    val stats = new PredicateSelectivity(tr)
    val optimizer = new PredicateSelectivityOptimizer
    val statsForOptimizer = PredPairAndBranchingStatistics(stats.mapOutOut, stats.mapInOut, stats.mapInIn, stats.mapOutIn, stats.mapPredicateBranching)

    val patterns1 = List(TriplePattern(x, p2, y), TriplePattern(y, p5, z), TriplePattern(x, p1, z1))
    val cardinalities1 = patterns1.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
    val queryWithCardinalities1 = OptimizableQueryWithStats(patterns1, cardinalities1)
    val optimizedQuery1 = optimizer.optimize(queryWithCardinalities1, statsForOptimizer)

    assert(optimizedQuery1 == List(TriplePattern(x, p1, z1), TriplePattern(x, p2, y), TriplePattern(y, p5, z)))
  }

  import TripleGenerators._
  implicit lazy val arbTriples = Arbitrary(genTriples map (_.toSet))
  implicit lazy val arbQuery = Arbitrary(queryPatterns)

  it should "correctly answer random queries with basic graph patterns" in {
    check((triples: Set[TriplePattern], queries: List[TriplePattern]) => {
      val tr = new TripleRush
      for (triple <- triples) {
        tr.addEncodedTriple(triple.s, triple.p, triple.o)
      }
      tr.prepareExecution
      val stats = new PredicateSelectivity(tr)
      val optimizer = new PredicateSelectivityOptimizer
      val statsForOptimizer = PredPairAndBranchingStatistics(stats.mapOutOut, stats.mapInOut, stats.mapInIn, stats.mapOutIn, stats.mapPredicateBranching)

      def calculateCardinalityOfPattern(tp: TriplePattern): Int = {
        val queryToGetCardinality = QuerySpecification(List(tp))
        val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality.toParticle)
        cardinalityQueryResult.size
      }

      val cardinalities = queries.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
      val queriesWithCardinalities = OptimizableQueryWithStats(queries, cardinalities)
      val optimizedQuery = optimizer.optimize(queriesWithCardinalities, statsForOptimizer)

      val trueOptimizedQuery = trueOptimizeQuery(queriesWithCardinalities, statsForOptimizer)
      val sortedPermutations = trueOptimizedQuery.toArray sortBy (_._2)
      assert(sortedPermutations.head._1 == optimizedQuery)
      
      //println("cardinalities: " + cardinalities.mkString(" "));
      //println(optimizedQuery)
      tr.shutdown
      true
    }, minSuccessful(20))
  }

  def trueOptimizeQuery(queriesWithCardinalities: PatternWithCardinality, statsForOptimizer: PredicatePairAndBranchingStatistics): scala.collection.mutable.Map[List[TriplePattern], Int] = {
    val allPermutations = queriesWithCardinalities.pattern.permutations
    val permutationsWithCost = scala.collection.mutable.Map[List[TriplePattern], Int]()

    for (permutation <- allPermutations) {
      var permutationCost = -1;
      var previousPattern = TriplePattern(0, 0, 0)

      for (tp <- permutation) {
        if (permutationCost < 0) permutationCost = queriesWithCardinalities.cardinality(tp)
        else {
          var explorationCost = (previousPattern.s, previousPattern.o) match {
            case (tp.s, _) => {
              queriesWithCardinalities.cardinality(previousPattern) * statsForOptimizer.cardinalityOutOut(previousPattern.p, tp.p)
            }
            case (tp.o, _) => {
              queriesWithCardinalities.cardinality(previousPattern) * statsForOptimizer.cardinalityOutIn(previousPattern.p, tp.p)
            }
            case (_, tp.o) => {
              queriesWithCardinalities.cardinality(previousPattern) * statsForOptimizer.cardinalityInIn(previousPattern.p, tp.p)
            }
            case (_, tp.s) => {
              queriesWithCardinalities.cardinality(previousPattern) * statsForOptimizer.cardinalityInOut(previousPattern.p, tp.p)
            }
            case (_, _) => {
              //println("none: "+predStatistics.cardinalityBranching(pattern.p) * predStatistics.cardinalityBranching(patternToExplore.p))
              statsForOptimizer.cardinalityBranching(previousPattern.p) * statsForOptimizer.cardinalityBranching(tp.p)
            }
          }
          permutationCost += explorationCost
        }
        previousPattern = tp
      }
      permutationsWithCost += permutation -> permutationCost
    }
    permutationsWithCost
  }

}
