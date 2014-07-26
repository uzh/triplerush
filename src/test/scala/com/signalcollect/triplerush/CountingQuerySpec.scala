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

class CountingQuerySpec extends FlatSpec with Checkers with TestAnnouncements {

  import TripleGenerators._

  implicit lazy val arbTriples = Arbitrary(genTriples map (_.toSet))
  implicit lazy val arbQuery = Arbitrary(queryPatterns)

  "Counting Query" should "correctly answer a query for data that is not in the store" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(1, 2, 3))
      val query = List(TriplePattern(-1, 4, -1))
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a query for a specific pattern that exists" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(1, 2, 3))
      val query = List(TriplePattern(1, 2, 3))
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "count zero results for an empty query" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(8, 23, 19), TriplePattern(13, 25, 5), TriplePattern(6, 23, 18))
      val query = List()
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a query for a specific pattern that does not exist" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(1, 2, 3))
      val query = List(TriplePattern(1, 4, 3))
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a query that successfully binds the same variable twice" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(1, 2, 1))
      val query = List(TriplePattern(-1, 2, -1))
      val trCount = TestHelper.count(tr, triples, query)
      assert(1 === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a query that successfully binds the same variable three times" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(1, 1, 1))
      val query = List(TriplePattern(-1, -1, -1))
      val trCount = TestHelper.count(tr, triples, query)
      assert(trCount === 1)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a query that unsuccessfully binds the same variable twice" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(1, 2, 3))
      val query = List(TriplePattern(-1, 2, -1))
      val trCount = TestHelper.count(tr, triples, query)
      assert(trCount === 0)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a query that unsuccessfully binds the same variable three times" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(1, 1, 2))
      val query = List(TriplePattern(-1, -1, -1))
      val trCount = TestHelper.count(tr, triples, query)
      assert(trCount === 0)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query 1" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(4, 3, 4))
      val query = List(TriplePattern(-1, 3, -1))
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query 2" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(3, 4, 2), TriplePattern(3, 4, 4), TriplePattern(2, 3, 3),
        TriplePattern(3, 3, 3), TriplePattern(1, 1, 2), TriplePattern(3, 3, 4),
        TriplePattern(4, 4, 1), TriplePattern(4, 4, 3))
      val query = List(TriplePattern(-2, -1, 3))
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query, where one pattern is fully bound and that triple exists" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(3, 4, 2), TriplePattern(3, 4, 4), TriplePattern(2, 3, 3),
        TriplePattern(3, 3, 3), TriplePattern(1, 1, 2), TriplePattern(3, 3, 4),
        TriplePattern(4, 4, 1), TriplePattern(4, 4, 3))
      val query = List(TriplePattern(3, 4, 2), TriplePattern(-2, -1, -3))
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount, s"Bindings found: ${trResults.size}, counting query results: $trCount")
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query, where one pattern is fully bound and that triple does not exist" in {
    val tr = new TripleRush
    try {
      val triples = Set(TriplePattern(3, 4, 2), TriplePattern(3, 4, 4), TriplePattern(2, 3, 3),
        TriplePattern(3, 3, 3), TriplePattern(1, 1, 2), TriplePattern(3, 3, 4),
        TriplePattern(4, 4, 1), TriplePattern(4, 4, 3))
      val query = List(TriplePattern(1, 2, 3), TriplePattern(-2, -1, 3))
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer a simple query over a lot of data" in {
    val triples = {
      for {
        s <- 1 to 25
        p <- 1 to 25
        o <- 1 to 25
      } yield TriplePattern(s, p, o)
    }.toSet

    val tr = new TripleRush
    try {
      val query = List(TriplePattern(-1, 1, -1), TriplePattern(-1, 2, -2), TriplePattern(-1, -3, 25))
      val trCount = TestHelper.count(tr, triples, query)
      val trResults = TestHelper.execute(tr, triples, query)
      assert(trResults.size === trCount)
    } finally {
      tr.shutdown
    }
  }

  it should "compute predicate selectivities over some triples" in {
    val tr = new TripleRush
    val jena = new Jena
    try {
      val triples = {
        for {
          s <- 1 to 2
          p <- 1 to 2
          o <- 1 to 10
        } yield TriplePattern(s, p, o)
      }.toSet
      val query = List(TriplePattern(-1, 1, -1), TriplePattern(-1, 2, -2), TriplePattern(-1, -3, 10))
      val trResults = TestHelper.execute(
        tr,
        triples,
        query)
    } finally {
      tr.shutdown
      jena.shutdown
    }
  }

  it should "also work with encoded triples" in {
    val tr = new TripleRush
    try {
      tr.addTriple("Elvis", "inspired", "Dylan")
      tr.addTriple("Dylan", "inspired", "Jobs")
      tr.prepareExecution
      val encodedInspired = tr.dictionary("inspired")
      val query = Seq(TriplePattern(-1, encodedInspired, -2))
      val countOptionFuture = tr.executeCountingQuery(query)
      val countOption = Await.result(countOptionFuture, 1.second)
      assert(countOption.isDefined === true)
      assert(countOption.get === 2)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly answer random queries with basic graph patterns" in {
    check(
      Prop.forAllNoShrink(tripleSet, queryPatterns) {
        (triples: Set[TriplePattern], query: List[TriplePattern]) =>
          val tr = new TripleRush
          try {
            val trCount = TestHelper.count(tr, triples, query)
            val trResults = TestHelper.execute(tr, Set(), query)
            assert(trResults.size === trCount, s"Bindings found: ${trResults.size}, counting query results: $trCount")
            trResults.size === trCount
          } finally {
            tr.shutdown
          }
      }, minSuccessful(5))
  }

}
