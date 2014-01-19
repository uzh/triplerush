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
import org.openrdf.query.QueryResult
import org.scalacheck.Prop.BooleanOperators
import com.signalcollect.triplerush.jena.Jena

class ResultCountingSpec extends FlatSpec with Checkers {

  import TripleGenerators._

  implicit lazy val arbTriples = Arbitrary(genTriples map (_.toSet))
  implicit lazy val arbQuery = Arbitrary(queryPatterns)

  "Counting queries" should "report the same number of results as a normal query" in {
    check((triples: Set[TriplePattern], query: List[TriplePattern]) => {
      val tr = new TripleRush
      for (triple <- triples) {
        tr.addEncodedTriple(triple.s, triple.p, triple.o)
      }
      tr.prepareExecution
      val q = QuerySpecification(query)
      val numberOfResultBindings = tr.executeQuery(q).size
      val countFuture = tr.executeCountingQuery(q)
      val count = Await.result(countFuture, 7200.seconds)
      tr.shutdown
      count === numberOfResultBindings
    }, minSuccessful(1000))
  }

}
