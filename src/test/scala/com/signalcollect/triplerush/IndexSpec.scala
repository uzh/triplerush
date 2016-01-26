/*
 *  @author Philip Stutz
 *  @author Bibek Paudel
 *
 *  Copyright 2014 University of Zurich
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
import scala.concurrent.duration.Duration

import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.NoArg
import org.scalatest.fixture.UnitFixture

import akka.testkit.EventFilter

class IndexSpec extends FlatSpec with UnitFixture {

  "TripleRush" should "correctly answer all 1 pattern queries when there is only 1 triple in the store" in new TestStore {
    val queries = {
      for {
        s <- Array(1, -1)
        p <- Array(2, -2)
        o <- Array(3, -3)
      } yield TriplePattern(s, p, o)
    }
    tr.addEncodedTriple(1, 2, 3)
    for (query <- queries.par) {
      val resultIterator = tr.resultIteratorForQuery(Seq(query))
      val trResult: Option[Array[Int]] = if (resultIterator.hasNext) Some(resultIterator.next) else None
      assert(!resultIterator.hasNext, "Query should have no more than 1 result.")
      assert(query.isFullyBound || trResult.isDefined, s"query $query should lead to 1 set of bindings, but there are none.")
      if (!query.isFullyBound) {
        val bindings = TestHelper.resultsToBindings(trResult.iterator).head
        assert(bindings.size == query.variables.size)
        if (bindings.size > 0) {
          if (query.s == -1) {
            assert(bindings.contains(-1), s"query $query has an unbound subject, but the result $bindings does not contain a binding for it.")
            assert(bindings(-1) == 1, s"query $query got bindings $bindings, which is wrong, the subject should have been bound to 1.")
          }
          if (query.p == -2) {
            assert(bindings.contains(-2), s"query $query has an unbound predicate, but the result $bindings does not contain a binding for it.")
            assert(bindings(-2) == 2, s"query $query got bindings $bindings, which is wrong, the predicate should have been bound to 2.")
          }
          if (query.o == -3) {
            assert(bindings.contains(-3), s"query $query has an unbound object, but the result $bindings does not contain a binding for it.")
            assert(bindings(-3) == 3, s"query $query got bindings $bindings, which is wrong, the object should have been bound to 3.")
          }
        }
      }
    }
  }

  it should "throw a helpful error when a required index is missing" in new NoArg {
    val graphBuilder = TestStore.instantiateUniqueGraphBuilder(testEventLogger = true)
    val indexStructure = new IndexStructure {
      def parentIds(pattern: TriplePattern): Set[TriplePattern] = Set.empty
    }
    val tr = TripleRush(graphBuilder = graphBuilder, indexStructure = indexStructure)
    implicit val system = tr.graph.system
    try {
      tr.addEncodedTriple(1, 2, 3)
      EventFilter[UnsupportedOperationException](occurrences = 1) intercept {
        tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
      }
    } finally {
      tr.close
      Await.result(system.terminate, Duration.Inf)
    }
  }

  it should "be able to answer a simple query with a partial index" in new NoArg {
    val graphBuilder = TestStore.instantiateUniqueGraphBuilder()
    val indexStructure = new IndexStructure {
      def parentIds(pattern: TriplePattern): Set[TriplePattern] = {
        pattern match {
          case fullyBound if fullyBound.isFullyBound => Set(fullyBound.copy(o = 0))
          case other @ _                             => Set.empty
        }
      }
    }
    val tr = TripleRush(graphBuilder = graphBuilder, indexStructure = indexStructure)
    implicit val system = tr.graph.system
    try {
      tr.addEncodedTriple(1, 2, 3)
      val results = tr.resultIteratorForQuery(Seq(TriplePattern(1, 2, -1))).size
      assert(results == 1)
    } finally {
      tr.close
      Await.result(system.terminate, Duration.Inf)
    }
  }

  it should "be able to load a million entries into a Splay-tree based index vertex" in new NoArg {
    val graphBuilder = TestStore.instantiateUniqueGraphBuilder()
    val indexStructure = new IndexStructure {
      def parentIds(pattern: TriplePattern): Set[TriplePattern] = {
        pattern match {
          case fullyBound if fullyBound.isFullyBound => Set(fullyBound.copy(o = 0))
          case other @ _                             => Set.empty
        }
      }
    }
    val tr = TripleRush(graphBuilder = graphBuilder, indexStructure = indexStructure)
    implicit val system = tr.graph.system
    try {
      val numTriples = 1000000
      // Reverse ordering to test a less conventional case.
      for (i <- numTriples to 1 by -1) {
        tr.addEncodedTriple(1, 2, i)
      }
      val results = tr.resultIteratorForQuery(Seq(TriplePattern(1, 2, -1))).size
      assert(results == numTriples)
    } finally {
      tr.close
      Await.result(system.terminate, Duration.Inf)
    }
  }

  it should "be able to load ten thousand entries into an array based index vertex" in new NoArg {
    val graphBuilder = TestStore.instantiateUniqueGraphBuilder()
    val indexStructure = new IndexStructure {
      def parentIds(pattern: TriplePattern): Set[TriplePattern] = {
        pattern match {
          case fullyBound if fullyBound.isFullyBound => Set(fullyBound.copy(p = 0))
          case other @ _                             => Set.empty
        }
      }
    }
    val tr = TripleRush(graphBuilder = graphBuilder, indexStructure = indexStructure)
    implicit val system = tr.graph.system
    try {
      val numTriples = 10000
      // Reverse ordering to test a less conventional case.
      for (i <- numTriples to 1 by -1) {
        tr.addEncodedTriple(1, i, 2)
      }
      val results = tr.resultIteratorForQuery(Seq(TriplePattern(1, -1, 2))).size
      assert(results == numTriples)
    } finally {
      tr.close
      Await.result(system.terminate, Duration.Inf)
    }
  }

}
