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

class QueryOptimizerSpec extends FlatSpec with ShouldMatchers with Checkers {
 
  "QueryOptimizer" should "put the minimum cardinality pattern first" in {
    check(
      (q: OptimizableQuery) => {
        val optimizer = new JoinStatisticsOptimizer
        val optimizedQ = optimizer.optimize(q)

        if(!q.pattern.isEmpty) {
          val minCard = q.cardinalities.values.min
          assert(q.pattern.length == optimizedQ.length, "Optimized Query cannot drop patterns that were in the original query.")
          val firstOptimizedPattern = optimizedQ.head
          assert(minCard == q.cardinality(firstOptimizedPattern), s"FirstOptimizedPattern has to have lowest cardinality, $minCard")
        }
        
        true
      },
      minSuccessful(100))
  }
  
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
        frequency((5, variableWithout(s)), (100, predicates)) //(1, variable), 
      } else {
        frequency((1, variable), (5, predicates))
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

  lazy val optimizableQuery = for{
	  patterns <- queryPatterns
  } yield OptimizableQuery(patterns, patterns.map(tp => (tp, Random.nextInt(Integer.MAX_VALUE))).toMap)
  
  implicit lazy val arbOptimizableQuery = Arbitrary(optimizableQuery)
}