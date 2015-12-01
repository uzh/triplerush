/*
 *  @author Philip Stutz
 *
 *  Copyright 2013 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.signalcollect.triplerush

import org.scalacheck.{ Arbitrary, Gen }
import org.scalacheck.Gen.{ const, containerOfN, freqTuple, frequency }
import org.scalacheck.Prop
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.TripleGenerators.{ queryPatterns, tripleSet }
import com.signalcollect.triplerush.jena.Jena
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RandomizedIntegrationSpec extends FlatSpec with Checkers {

  "TripleRush" should "correctly answer random queries with basic graph patterns" in {
    check(
      Prop.forAllNoShrink(tripleSet, queryPatterns) {
        (triples: Set[TriplePattern], query: List[TriplePattern]) =>
          val jena = new Jena()
          val tr = TestStore.instantiateUniqueStore()
          try {
            val jenaResults = TestHelper.execute(jena, triples, query)
            val trResults = TestHelper.execute(tr, triples, query)
            assert(jenaResults === trResults, s"Jena results $jenaResults did not equal our results $trResults.")
            jenaResults === trResults
          } finally {
            jena.close
            tr.close
            Await.result(tr.graph.system.terminate(), Duration.Inf)
          }
      }, minSuccessful(100))
  }

}

object TripleGenerators {
  val maxId = 25

  // Smaller ids are more frequent.
  lazy val frequencies = (1 to maxId) map (id => (maxId + 10 - id, const(id)))
  lazy val smallId = frequency(frequencies: _*)

  lazy val predicates = Gen.oneOf(23, 24, 25)

  lazy val x = -1
  lazy val y = -2
  lazy val z = -3
  lazy val a = -4
  lazy val b = -5
  lazy val c = -6

  val variableFrequencies = List((20, x), (5, y), (1, z), (1, a), (1, b), (1, c))

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

  lazy val genTriple = for {
    s <- smallId
    p <- predicates
    o <- smallId
  } yield TriplePattern(s, p, o)

  lazy val genTriples = containerOfN[List, TriplePattern](100, genTriple)
  lazy val tripleSet = genTriples map (_.toSet)

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
        frequency((5, variableWithout(s, p)), (1, variable), (1, smallId))
      } else if (s < 0) {
        // Try not having the same variable multiple times in a pattern too often.
        frequency((5, variableWithout(s)), (1, variable), (2, smallId))
      } else if (p < 0) {
        // Try not having the same variable multiple times in a pattern too often.
        frequency((5, variableWithout(p)), (1, variable), (2, smallId))
      } else {
        // No variables in pattern yet, need one.
        variable
      }
    } yield TriplePattern(s, p, o)
  }

  lazy val queryPatterns: Gen[List[TriplePattern]] = {
    for {
      p <- Arbitrary(genQueryPattern).arbitrary
      // Bias towards shorter pattern lists.
      patternList <- frequency((3, Nil), (2, queryPatterns))
    } yield (p :: patternList)
  }

}
