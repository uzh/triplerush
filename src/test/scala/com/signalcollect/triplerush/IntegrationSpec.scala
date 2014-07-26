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

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.QueryParticle._
import scala.util.Random
import scala.annotation.tailrec
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Prop
import org.scalacheck.Prop.BooleanOperators
import com.signalcollect.triplerush.jena.Jena
import com.signalcollect.triplerush.optimizers.GreedyCardinalityOptimizer

class IntegrationSpec extends FlatSpec with Checkers with TestAnnouncements {

  import TripleGenerators._

  implicit lazy val arbTriples = Arbitrary(tripleSet)
  implicit lazy val arbQuery = Arbitrary(queryPatterns)

  "TripleRush" should "correctly answer a query for data that is not in the store" in {
    val tr = new TripleRush
    try {
      val trResults = TestHelper.execute(
        tr,
        Set(TriplePattern(1, 2, 3)),
        List(TriplePattern(-1, 4, -1)))
      assert(Set[Map[Int, Int]]() === trResults)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a query for a specific pattern that exists" in {
    val tr = new TripleRush
    try {
      val trResults = TestHelper.execute(
        tr,
        Set(TriplePattern(1, 2, 3)),
        List(TriplePattern(1, 2, 3)))
      assert(Set[Map[Int, Int]](Map()) === trResults)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a query for a specific pattern that does not exist" in {
    val tr = new TripleRush
    try {
      val trResults = TestHelper.execute(
        tr,
        Set(TriplePattern(1, 2, 3)),
        List(TriplePattern(1, 4, 3)))
      assert(Set[Map[Int, Int]]() === trResults)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query 1" in {
    val tr = new TripleRush
    try {
      val trResults = TestHelper.execute(
        tr,
        Set(TriplePattern(4, 3, 4)),
        List(TriplePattern(-1, 3, -1)))
      assert(Set(Map(-1 -> 4)) === trResults)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query 2" in {
    val tr = new TripleRush
    try {
      val trResults = TestHelper.execute(
        tr,
        Set(TriplePattern(3, 4, 2), TriplePattern(3, 4, 4), TriplePattern(2, 3, 3),
          TriplePattern(3, 3, 3), TriplePattern(1, 1, 2), TriplePattern(3, 3, 4),
          TriplePattern(4, 4, 1), TriplePattern(4, 4, 3)),
        List(TriplePattern(-2, -1, 3)))
      assert(Set(Map(-1 -> 3, -2 -> 2), Map(-1 -> 3, -2 -> 3),
        Map(-1 -> 4, -2 -> 4)) === trResults)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query, where one pattern is fully bound and that triple exists" in {
    val tr = new TripleRush
    try {
      val trResults = TestHelper.execute(
        tr,
        Set(TriplePattern(3, 4, 2), TriplePattern(3, 4, 4), TriplePattern(2, 3, 3),
          TriplePattern(3, 3, 3)),
        List(TriplePattern(3, 4, 2), TriplePattern(-1, -2, -3)))
      assert(Set(Map(-1 -> 3, -2 -> 4, -3 -> 2), Map(-1 -> 3, -2 -> 4, -3 -> 4),
        Map(-1 -> 2, -2 -> 3, -3 -> 3), Map(-1 -> 3, -2 -> 3, -3 -> 3)) === trResults)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query, where one pattern is fully bound and that triple does not exist" in {
    val tr = new TripleRush
    try {
      val trResults = TestHelper.execute(
        tr,
        Set(TriplePattern(3, 4, 2), TriplePattern(3, 4, 4), TriplePattern(2, 3, 3),
          TriplePattern(3, 3, 3), TriplePattern(1, 1, 2), TriplePattern(3, 3, 4),
          TriplePattern(4, 4, 1), TriplePattern(4, 4, 3)),
        List(TriplePattern(1, 2, 3), TriplePattern(-2, -1, 3)))
      assert(Set[Map[Int, Int]]() === trResults)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query over a reasonable amount of data" in {
    val tr = new TripleRush
    val jena = new Jena
    try {
      val query = List(TriplePattern(-1, 1, -1), TriplePattern(-1, 2, -2), TriplePattern(-1, -3, 10))
      val triples = {
        for {
          s <- 1 to 3
          p <- 1 to 3
          o <- 1 to 10
        } yield TriplePattern(s, p, o)
      }.toSet
      val trResults = TestHelper.execute(
        tr,
        triples,
        query)
      val jenaResults = TestHelper.execute(
        jena,
        triples,
        query)
      assert(jenaResults === trResults, s"Jena results $jenaResults did not equal our results $trResults.")
    } finally {
      tr.shutdown
      jena.shutdown
    }
  }

  it should "correctly answer random queries with basic graph patterns" in {
    check(
      Prop.forAllNoShrink(tripleSet, queryPatterns) {
        (triples: Set[TriplePattern], query: List[TriplePattern]) =>
          val tr = new TripleRush
          val jena = new Jena
          try {
            val jenaResults = TestHelper.execute(jena, triples, query)
            val trResults = TestHelper.execute(tr, triples, query)
            assert(jenaResults === trResults, s"Jena results $jenaResults did not equal our results $trResults.")
            jenaResults === trResults
          } finally {
            tr.shutdown
            jena.shutdown
          }
      }, minSuccessful(10))
  }

  "ResultIteratorQueries" should "correctly answer random queries with basic graph patterns" in {
    check(
      Prop.forAllNoShrink(tripleSet, queryPatterns) {
        (triples: Set[TriplePattern], query: List[TriplePattern]) =>
          val tr = new TripleRush
          val jena = new Jena
          try {
            val jenaResults = TestHelper.execute(jena, triples, query)
            TestHelper.prepareStore(tr, triples)
            val resultIterator = tr.resultIteratorForQuery(query)
            val trResults = TestHelper.resultsToBindings(resultIterator.toList)
            assert(jenaResults === trResults, s"Jena results $jenaResults did not equal our results $trResults.")
            jenaResults === trResults
          } finally {
            tr.shutdown
            jena.shutdown
          }
      }, minSuccessful(20))
  }

}

object TestHelper {

  def prepareStore(qe: QueryEngine,
    triples: Set[TriplePattern]) {
    for (triple <- triples) {
      qe.addEncodedTriple(triple.s, triple.p, triple.o)
    }
    qe.prepareExecution
  }

  def count(tr: TripleRush,
    triples: Set[TriplePattern],
    query: List[TriplePattern]): Long = {
    prepareStore(tr, triples)
    val resultFuture = tr.executeCountingQuery(query, Some(GreedyCardinalityOptimizer))
    val result = Await.result(resultFuture, 7200.seconds).get //we assume the query execution is complete
    result
  }

  def resultsToBindings(results: Traversable[Array[Int]]): Set[Map[Int, Int]] = {
    results.map({ binding: Array[Int] =>
      // Only keep variable bindings that have an assigned value.
      val filtered: Map[Int, Int] = {
        (-1 to -binding.length by -1).
          zip(binding).
          filter(_._2 > 0).
          toMap
      }
      filtered
    }).toSet
  }

  def execute(
    qe: QueryEngine,
    triples: Set[TriplePattern],
    query: List[TriplePattern]): Set[Map[Int, Int]] = {
    prepareStore(qe, triples)
    val results = qe.executeQuery(query)
    val bindings = resultsToBindings(results)
    bindings
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
