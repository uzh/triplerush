package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import com.signalcollect.triplerush.optimizers.PredicateSelectivityEdgeCountsOptimizer
import scala.util.Random
import org.scalacheck.Gen._

class PredectivitySelectivityEdgeCountsOptimizerSpec extends FlatSpec with Checkers {

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

  "PredicateSelectivityEdgeCountsOptimizer" should "correctly find the optimal query order" in {
    val tr = new TripleRush
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
    //tr.addEncodedTriple(o3, p4, o9)

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
    val edgeCounts = Map(TriplePattern(0, p1, 0) -> 2l, TriplePattern(0, p3, 0) -> 8l, TriplePattern(0, p4, 0) -> 6l)
    println("inIn: "+stats.inIn.toString)
    println("inOut: "+stats.inOut.toString)
    //println(stats.outIn.mkString(", "))
    println("outOut: "+stats.outOut.toString)
    
    val optimizedQuery = optimizer.optimize(cardinalities, Some(edgeCounts))
    
    //assert(optimizedQuery.toList == List(TriplePattern(x, p2, y), TriplePattern(y, p4, z)))
    tr.shutdown

  }

  /*
  import TripleGenerators._
  lazy val genTriplesMore = containerOfN[List, TriplePattern](10000, genTriple)
  implicit lazy val arbTriples = Arbitrary(genTriplesMore map (_.toSet))
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

        println("cardinalities: " + cardinalities.toList.mkString(" "))
        println("edgeCounts: " + edgeCounts.toList.mkString(" "))
        println("optimized: " + optimizedQuery.toList)
      }
      tr.shutdown
      true
    }, minSuccessful(10000))
  }
  * */

}