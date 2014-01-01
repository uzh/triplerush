package com.signalcollect.triplerush

import scala.util.Random
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import org.scalatest.prop.Checkers
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import org.scalacheck.Arbitrary

class QueryOptimizerSpec2 extends FlatSpec with ShouldMatchers with Checkers {

  "QueryOptimizer" should "put the minimum cardinality pattern first" in {
    check(
      (q: OptimizableQueryWithStats) => {

        def randomInRange(low: Int) : Int = {
          val range = low to maxCardinality
          range(Random.nextInt(range length))
        }
        def randomInRangeMax(low1: Int, low2: Int) : Int = {
          val range =  math.max(low1, low2) to maxCardinality
          range(Random.nextInt(range length))
          //(math.max(low1, low2) to Integer.MAX_VALUE)(Random.nextInt(math.max(low1, low2) to Integer.MAX_VALUE length))        
        }
        def randomInRangeCeiling(high: Int) : Int = {
          val range = 1 to high
          range(Random.nextInt(range length))
        }

        val listOfPredicates = q.pattern.map(_.p)
        val range = 100 to 200
        /*val statistics = PredStatistics2(
            //q.pattern.map(tp => (tp.p, (q.cardinality(tp) to Integer.MAX_VALUE)(Random.nextInt(q.cardinality(tp) to Integer.MAX_VALUE length)))).toMap,
          q.pattern.map(tp => (tp.p, randomInRange(q.cardinality(tp)))).distinct.toMap , 
          		//cardinalities of predicates, should be at least equal to the cardinality
          q.pattern.map(tp => (tp.p, randomInRangeCeiling(q.cardinality(tp)))).distinct.toMap, 
          		//cardinalities of sub-pred, should be at least equal to the cardinality
          q.pattern.map(tp => (tp.p, randomInRangeCeiling(q.cardinality(tp)))).distinct.toMap) 
          		//cardinalities of obj-pred, should be at least equal to the cardinality
		*/
        
        val listOfPredicatePairs = (for{
          x <- listOfPredicates; y <- listOfPredicates
          //if(listOfPredicates.indexOf(x) != listOfPredicates.indexOf(y))
        } yield(x,y)).distinct
        
        val predOutOut = listOfPredicatePairs.map(
            pair => (PredicatePair(pair._1, pair._2), randomInRangeMax(pair._1, pair._2))).toMap
            //pair => (MapKey(pair._1, pair._2), 
              //  (math.max(statistics.cardinalityOfPredicate(pair._1), statistics.cardinalityOfPredicate(pair._2)) to Integer.MAX_VALUE)
                //	(Random.nextInt(math.max(statistics.cardinalityOfPredicate(pair._1), statistics.cardinalityOfPredicate(pair._2)) to Integer.MAX_VALUE length))))
        val predInOut = listOfPredicatePairs.map(
            pair => (PredicatePair(pair._1, pair._2), randomInRangeMax(pair._1, pair._2))).toMap
        val predInIn = listOfPredicatePairs.map(
            pair => (PredicatePair(pair._1, pair._2), randomInRangeMax(pair._1, pair._2))).toMap
        val predOutIn = listOfPredicatePairs.map(
            pair => (PredicatePair(pair._1, pair._2), randomInRangeMax(pair._1, pair._2))).toMap
        //val listOfPredicates = for (pattern <- q.pattern) yield pattern.map(_.p)
        //val listOfPredicatePairs = for (x <- listOfPredicates.values; y <- listOfPredicates) yield (x,y)
            
        val predicateBranching = q.pattern.map(tp => (tp.p, randomInRange(q.cardinality(tp)))).distinct.toMap
        //val predicateBranching = listOfPredicates.map(predicate => (predicate, randomInRange(q.cardinality(predicate)))).toMap
            
        val predPairStatistics = new PredPairAndBranchingStatistics(predOutOut, predInOut, predInIn, predOutIn, predicateBranching)
        val optimizer = new JoinOptimizerWithStatistics
        //val optimizedQ = optimizer.optimize(q)
        //val optimizedQ1 = optimizer.optimizeQ(q, statistics, predPairStatistics)
        val optimizedQ1 = optimizer.optimize(q, predPairStatistics)

        //if (!q.pattern.isEmpty) {
        if (q.pattern.size > 1) {
          //println("args0: "+q.pattern.mkString(" ")+", cards: "+q.cardinalities.mkString(" ")+", args1: "+statistics.toString+", args2: "+predPairStatistics.toString);
          println("args0: "+q.cardinalities.mkString(" "));
          println("optimized: "+optimizedQ1.mkString(" "))
          val minCard = q.cardinalities.values.min
          assert(q.pattern.length == optimizedQ1.length, "Optimized Query cannot drop patterns that were in the original query.")
          val firstOptimizedPattern = optimizedQ1.head
          assert(minCard == q.cardinality(firstOptimizedPattern), s"FirstOptimizedPattern has to have lowest cardinality, $minCard")
        }

        true
      },
      minSuccessful(5))
      //minSuccessful(100))
  }

  val maxCardinality = 100000
  val maxId = 25
  // Smaller ids are more frequent.
  lazy val frequencies = (1 to maxId) map (id => (maxId + 10 - id, const(id)))
  lazy val smallId = frequency(frequencies: _*)

  lazy val predicates = Gen.oneOf(23, 24, 25)

  lazy val x = -1
  lazy val y = -2
  lazy val z = -3

  val variableFrequencies = List((20, x), (5, y), (1, z))

  // Takes a list of (frequency, value) tuples and turns the values into generators.
  def frequenciesToGenerator[G](f: List[(Int, G)]): List[(Int, Gen[G])] = {
    f.map { t: (Int, G) => ((t._1, const(t._2))) }
  }

  // Different frequencies for different variables.
  lazy val variable = {
    val varGenerators = frequenciesToGenerator(variableFrequencies)
    frequency(varGenerators: _*)
  }

  // Returns a variable, but not any in vs.
  def variableWithout(vs: Int*) = {
    val vSet = vs.toSet
    val filteredVars = frequenciesToGenerator(variableFrequencies.filter {
      t => !vs.contains(t._2)
    })
    frequency(filteredVars: _*)
  }

  lazy val genQueryPattern: Gen[TriplePattern] = {
    for {
      s <- frequency((2, variable), (1, smallId))
      // Only seldomly add variables in predicate position, expecially if the subject is a variable.
      p <- if (s < 0) {
        // Only seldomly have the same variable multiple times in a pattern.
        //frequency((5, variableWithout(s)), (100, predicates)) //(1, variable), 
        frequency((0, variableWithout(s)), (100, predicates)) //(1, variable), 
      } else {
        //frequency((1, variable), (5, predicates))
        frequency((0, variable), (5, predicates))
      }
      o <- if (s < 0 && p < 0) {
        smallId
      } else if (s < 0) {
        // Try not having the same variable multiple times in a pattern too often.
        frequency((1, variableWithout(s)), (1, variable), (2, smallId))
      } else if (p < 0) {
        // Try not having the same variable multiple times in a pattern too often.
        frequency((1, variableWithout(p)), (1, variable), (2, smallId))
      } else {
        // No variables in pattern yet, need one.
        variable
      }
    } yield TriplePattern(s, p, o)
  }

  lazy val queryPatterns: Gen[List[TriplePattern]] = containerOf[List, TriplePattern](genQueryPattern)
  lazy val resizedQueryPatterns = Gen.resize(6, queryPatterns)
  
  lazy val listOfPredicates = for (patterns <- queryPatterns) yield patterns.map(_.p)

  lazy val optimizableQuery = for {
    //patterns <- queryPatterns
    patterns <- resizedQueryPatterns
  } yield OptimizableQueryWithStats(patterns.distinct,
    patterns.map(tp => (tp, Random.nextInt(maxCardinality))).toMap) // cardinalities

  implicit lazy val arbOptimizableQuery = Arbitrary(optimizableQuery)

}