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
    val optimizedQuery = optimizer.optimize(queryWithCardinalities, statsForOptimizer, false)
    assert(optimizedQuery == List(TriplePattern(x, p2, y), TriplePattern(y, p4, z)))
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
    val optimizer = new PredicateSelectivityOptimizer
    val statsForOptimizer = PredPairAndBranchingStatistics(stats.mapOutOut, stats.mapInOut, stats.mapInIn, stats.mapOutIn, stats.mapPredicateBranching)

    val patterns1 = List(TriplePattern(x, p2, y), TriplePattern(y, p5, z), TriplePattern(x, p1, z1))
    val cardinalities1 = patterns1.map(tp => (tp, calculateCardinalityOfPattern(tp))).toMap
    val queryWithCardinalities1 = OptimizableQueryWithStats(patterns1, cardinalities1)
    val optimizedQuery1 = optimizer.optimize(queryWithCardinalities1, statsForOptimizer, false)

    assert(optimizedQuery1 == List(TriplePattern(x, p1, z1), TriplePattern(x, p2, y), TriplePattern(y, p5, z)))
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
      
      if(cardinalities.forall(_._2 > 0) && cardinalities.size > 1 && cardinalities.forall(_._1.p > 0)){
      println("cardinalities: " + cardinalities.mkString(" "));
      val queriesWithCardinalities = OptimizableQueryWithStats(queries.distinct, cardinalities)
      val optimizer = new PredicateSelectivityOptimizer
      val statsForOptimizer = PredPairAndBranchingStatistics(stats.mapOutOut, stats.mapInOut, stats.mapInIn, stats.mapOutIn, stats.mapPredicateBranching)

      val optimizedQuery = optimizer.optimize(queriesWithCardinalities, statsForOptimizer, false)
      
      val trueOptimizedQuery = trueOptimizeQuery(queriesWithCardinalities, statsForOptimizer)
      //val sortedPermutations = trueOptimizedQuery.toArray sortBy (_._2)
      val sortedPermutations = trueOptimizedQuery.toArray.sortWith(comparePatterns) 
      println("optimized query: "+optimizedQuery)
      if(sortedPermutations.head._1 == optimizedQuery)
        println("FOUND")
      else {
        println("NOT FOUND: true: "+sortedPermutations.head._1+" ("+sortedPermutations.head._2+"), cost of returned order: "+trueOptimizedQuery(optimizedQuery))
        optimizer.optimize(queriesWithCardinalities, statsForOptimizer, true)
      }
      assert(sortedPermutations.head._1 == optimizedQuery)
      }
      tr.shutdown
      true
    }, minSuccessful(20))
  }

  def comparePatterns(tp1: (List[TriplePattern], Int), tp2: (List[TriplePattern], Int)) = {
    if(tp1._2 == tp2._2){
      if(tp1._1.head.p == tp2._1.head.p){
        if(tp1._1.head.s == tp2._1.head.s){
          (tp1._1.head.o < tp2._1.head.o)
        }
        else
          (tp1._1.head.s < tp2._1.head.s)
      }
      else
        (tp1._1.head.p < tp2._1.head.p)
    }
    else
      (tp1._2 < tp2._2)
  }
  
  def trueOptimizeQuery(queriesWithCardinalities: PatternWithCardinality, statsForOptimizer: PredicatePairAndBranchingStatistics): scala.collection.mutable.Map[List[TriplePattern], Int] = {
    val allPermutations = queriesWithCardinalities.pattern.permutations
    val permutationsWithCost = scala.collection.mutable.Map[List[TriplePattern], Int]()
    var emptyQuery: Boolean = false
    
    for (permutation <- allPermutations) {
      var permutationCost = -1;
      var previousPattern = TriplePattern(0, 0, 0)
      
      for (tp <- permutation) {
        var updatedCardinalities = scala.collection.mutable.Map[TriplePattern, Int]()
        for(tp1<- permutation){
        			updatedCardinalities += tp1 -> queriesWithCardinalities.cardinality(tp1)
        }
        
        //if (permutationCost < 0) permutationCost = queriesWithCardinalities.cardinality(tp)
        if (permutationCost < 0) permutationCost = 0
        else {
          var explorationCost = (previousPattern.s, previousPattern.o) match {
            case (tp.s, _) => {
              if(statsForOptimizer.cardinalityOutOut(previousPattern.p, tp.p)!=0)
            	  updatedCardinalities(previousPattern) * statsForOptimizer.cardinalityOutOut(previousPattern.p, tp.p)
              else {
                emptyQuery = true
                updatedCardinalities(previousPattern) * queriesWithCardinalities.cardinality(tp) 
              }
            }
            case (tp.o, _) => {
              if(statsForOptimizer.cardinalityOutIn(previousPattern.p, tp.p)!=0)
            	  updatedCardinalities(previousPattern) * statsForOptimizer.cardinalityOutIn(previousPattern.p, tp.p)
              else {
                emptyQuery = true
                updatedCardinalities(previousPattern) * queriesWithCardinalities.cardinality(tp)
              }
            }
            case (_, tp.o) => {
              if(statsForOptimizer.cardinalityInIn(previousPattern.p, tp.p) !=0)
            	  updatedCardinalities(previousPattern) * statsForOptimizer.cardinalityInIn(previousPattern.p, tp.p)
              else {
                emptyQuery = true
                updatedCardinalities(previousPattern) * queriesWithCardinalities.cardinality(tp)
              }
            }
            case (_, tp.s) => {
              if(statsForOptimizer.cardinalityInOut(previousPattern.p, tp.p) !=0)
            	  updatedCardinalities(previousPattern) * statsForOptimizer.cardinalityInOut(previousPattern.p, tp.p)
              else {
                emptyQuery = true
                updatedCardinalities(previousPattern) * queriesWithCardinalities.cardinality(tp)
              }
            }
            case (_, _) => {
              //println("none: "+predStatistics.cardinalityBranching(pattern.p) * predStatistics.cardinalityBranching(patternToExplore.p))
              statsForOptimizer.cardinalityBranching(previousPattern.p) * statsForOptimizer.cardinalityBranching(tp.p)
            }
          }
          if(explorationCost == 0)
            permutationCost += updatedCardinalities(previousPattern)
          permutationCost += explorationCost
          //updatedCardinalities(previousPattern) = explorationCost
          updatedCardinalities(previousPattern) = permutationCost
        }
        previousPattern = tp
        if(emptyQuery)
          permutationsWithCost += List() -> 0
      }
      permutationsWithCost += permutation -> permutationCost
    }
    permutationsWithCost
  }

}
