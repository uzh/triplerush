package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import com.signalcollect.triplerush.optimizers.PredicateSelectivityEdgeCountsOptimizer
import scala.util.Random
import org.scalacheck.Gen._

class PredectivitySelectivityEdgeCountsOptimizerSpec extends FlatSpec with Checkers {

  /*
  val s1 = 1
  val s2 = 2
  val s3 = 3
  val s4 = 4
  val s5 = 5
  val s6 = 6
  val s7 = 7
  val s8 = 8
  val s9 = 9
  val s10 = 10
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
  val p6 = 1006
  val p7 = 1007
  val p8 = 1008
  val p9 = 1009
  val p10 = 1010

  val x = -1
  val y = -2
  val z = -3
  val z1 = -4
	*/
  
  /*
  "PredicateSelectivityEdgeCountsOptimizer" should "order the patterns in queries " in {
    val tr = new TripleRush
    tr.addEncodedTriple(s1, p1, o1)
    tr.addEncodedTriple(s2, p1, o2)
    tr.addEncodedTriple(s1, p2, o3)
    tr.addEncodedTriple(s1, p2, o4)
    tr.addEncodedTriple(s3, p2, o10)
    tr.addEncodedTriple(s2, p3, o5)
    tr.addEncodedTriple(s4, p3, o5)
    tr.addEncodedTriple(s5, p3, o5)
    tr.addEncodedTriple(s6, p3, o5)
    tr.addEncodedTriple(s7, p3, o5)
    tr.addEncodedTriple(s8, p3, o5)
    tr.addEncodedTriple(s9, p3, o5)
    tr.addEncodedTriple(o5, p4, o6)
    tr.addEncodedTriple(o4, p4, o7)
    tr.addEncodedTriple(o3, p4, o8)
    tr.addEncodedTriple(o10, p4, o11)
    tr.addEncodedTriple(o3, p5, o9)
    tr.addEncodedTriple(o10, p5, o9)
    tr.addEncodedTriple(o10, p6, o9)
    tr.addEncodedTriple(o10, p7, o9)
    tr.addEncodedTriple(o10, p8, o9)
    tr.addEncodedTriple(o10, p9, o9)
    tr.addEncodedTriple(o10, p10, o9)
    tr.prepareExecution

    def calculateCardinalityOfPattern(tp: TriplePattern): Long = {
      val queryToGetCardinality = QuerySpecification(List(tp))
      val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
      cardinalityQueryResult.size
    }

    var r = new scala.util.Random(1000L)
    
    val stats = new PredicateSelectivity(tr)
    val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)

    val patterns = List(TriplePattern(y, p4, z), TriplePattern(x, p2, y))
    val cardinalities = patterns.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
    val edgeCounts = patterns.map(tp => (TriplePattern(0, tp.p, 0), scala.util.Random.nextLong)).toMap
    
    val optimizedQuery = optimizer.optimize(cardinalities, Some(edgeCounts))
    assert(optimizedQuery.toList == List(TriplePattern(x, p2, y), TriplePattern(y, p4, z)))
    tr.shutdown
  }

  "PredicateSelectivityEdgeCountsOptimizer" should "order the patterns in another query " in {
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

    def calculateCardinalityOfPattern(tp: TriplePattern): Long = {
      val queryToGetCardinality = QuerySpecification(List(tp))
      val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
      cardinalityQueryResult.size
    }

    val stats = new PredicateSelectivity(tr)
    val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)

    val patterns1 = List(TriplePattern(x, p2, y), TriplePattern(y, p5, z), TriplePattern(x, p1, z1))
    val cardinalities1 = patterns1.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
    val optimizedQuery1 = optimizer.optimize(cardinalities1, Some(cardinalities1))

    assert(optimizedQuery1.toList == List(TriplePattern(x, p1, z1), TriplePattern(x, p2, y), TriplePattern(y, p5, z)))
    tr.shutdown
  }*/

  import TripleGenerators._
  lazy val genTriplesMore = containerOfN[List, TriplePattern](10000, genTriple)
  implicit lazy val arbTriples = Arbitrary(genTriplesMore map (_.toSet))
  //implicit lazy val arbQuery = Arbitrary(queryPatterns)
  implicit lazy val genQueries = containerOfN[List, TriplePattern](10, genQueryPattern)
  implicit lazy val arbQuery = Arbitrary(genQueries)
  
  it should "correctly answer random queries with basic graph patterns" in {
    
    readLine
    
    check((triples: Set[TriplePattern], queries: List[TriplePattern]) => {
      val tr = new TripleRush
      for (triple <- triples) {
        tr.addEncodedTriple(triple.s, triple.p, triple.o)
      }
      tr.prepareExecution
      val stats = new PredicateSelectivity(tr)

      def calculateCardinalityOfPattern(tp: TriplePattern): Long = {
        val queryToGetCardinality = QuerySpecification(List(tp))
        val cardinalityQueryResult = tr.executeQuery(queryToGetCardinality)
        cardinalityQueryResult.size
      }

      val r = new scala.util.Random(1000L)
      val cardinalities = queries.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
      val edgeCounts = queries.map(tp => (TriplePattern(0, tp.p, 0), r.nextInt(100).toLong)).toMap

      if (cardinalities.forall(_._2 > 0) && cardinalities.size > 1 && cardinalities.forall(_._1.p > 0)) {
        val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)
        val optimizedQuery = optimizer.optimize(cardinalities, Some(edgeCounts))

        println("cardinalities: "+cardinalities.toList.mkString(" "))
        println("edgeCounts: "+edgeCounts.toList.mkString(" "))
        println("optimized: "+optimizedQuery.toList)
      }
      tr.shutdown
      true
    }, minSuccessful(10000))
  }

  
}