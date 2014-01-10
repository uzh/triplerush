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
    val optimizer = new PredicateSelectivityOptimizer(stats, debug = true)

    val patterns = List(TriplePattern(y, p4, z), TriplePattern(x, p2, y))
    val cardinalities = patterns.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
    val optimizedQuery = optimizer.optimize(cardinalities)
    assert(optimizedQuery.toList == List(TriplePattern(x, p2, y), TriplePattern(y, p4, z)))
    tr.shutdown
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
    val optimizer = new PredicateSelectivityOptimizer(stats, debug = true)

    val patterns1 = List(TriplePattern(x, p2, y), TriplePattern(y, p5, z), TriplePattern(x, p1, z1))
    val cardinalities1 = patterns1.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
    val optimizedQuery1 = optimizer.optimize(cardinalities1)

    assert(optimizedQuery1.toList == List(TriplePattern(x, p1, z1), TriplePattern(x, p2, y), TriplePattern(y, p5, z)))
    tr.shutdown
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

      def calculateCardinalityOfPattern(tp: TriplePattern): Int = {
        val queryToGetCardinality = QuerySpecification(List(tp))
        val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality.toParticle)
        cardinalityQueryResult.size
      }

      val cardinalities = queries.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap

      if (cardinalities.forall(_._2 > 0) && cardinalities.size > 1 && cardinalities.forall(_._1.p > 0)) {
        val optimizer = new PredicateSelectivityOptimizer(stats, debug = true)
        val optimizedQuery = optimizer.optimize(cardinalities)
        val trueOptimizedQuery = trueOptimizeQuery(cardinalities, stats)
        val sortedPermutations = trueOptimizedQuery.toArray sortBy (_._2)

        println("cardinalities: "+cardinalities.toList.mkString(" "))
        println("optimized: "+optimizedQuery.toList)
      if(sortedPermutations.head._2 >= trueOptimizedQuery(optimizedQuery.toList))  
        println("FOUND")
      else {
        println("NOT FOUND: true: "+sortedPermutations.head._1+" ("+sortedPermutations.head._2+"), cost of returned order: "+trueOptimizedQuery(optimizedQuery.toList))
      }
        //assert(sortedPermutations.head._1 == optimizedQuery.toList)
        assert(sortedPermutations.head._2 >= trueOptimizedQuery(optimizedQuery.toList) || (sortedPermutations.head._1.toList == optimizedQuery.toList))
      }
      tr.shutdown
      true
    }, minSuccessful(20))
  }

  def trueOptimizeQuery(
    patternsWithCardinalities: Map[TriplePattern, Int],
    statsForOptimizer: PredicateSelectivity): scala.collection.mutable.Map[List[TriplePattern], Int] = {

    val allPermutations = patternsWithCardinalities.keys.toList.permutations
    val permutationsWithCost = scala.collection.mutable.Map[List[TriplePattern], Int]()

    for (permutation <- allPermutations) {
      var permutationCost = -1;
      var previousPattern = TriplePattern(0, 0, 0)
      var emptyQuery = false;
      for (tp <- permutation) {
        emptyQuery = false
        if (permutationCost >= 0) {
          var explorationCost = (previousPattern.s, previousPattern.o) match {
            case (tp.s, _) => patternsWithCardinalities(previousPattern) * statsForOptimizer.outOut(previousPattern.p, tp.p)
            case (tp.o, _) => patternsWithCardinalities(previousPattern) * statsForOptimizer.outIn(previousPattern.p, tp.p)
            case (_, tp.o) => patternsWithCardinalities(previousPattern) * statsForOptimizer.inIn(previousPattern.p, tp.p)
            case (_, tp.s) => patternsWithCardinalities(previousPattern) * statsForOptimizer.inOut(previousPattern.p, tp.p)
            case (_, _) => patternsWithCardinalities(previousPattern) * patternsWithCardinalities(tp)
          }
          permutationCost += explorationCost
          if (explorationCost == 0) {
            emptyQuery = true
          }
        } else {
          permutationCost += patternsWithCardinalities(tp)
        }
        previousPattern = tp
      }
      if (emptyQuery) {
        permutationsWithCost += permutation -> 0
        permutationsWithCost += List() -> 0
      }
      permutationsWithCost += permutation -> permutationCost
    }
    permutationsWithCost
  }
}
