/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
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

import java.util.concurrent.TimeUnit
import concurrent.Await
import concurrent.duration.FiniteDuration
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.GraphBuilder
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.triplerush.evaluation.SparqlDsl.SELECT
import com.signalcollect.triplerush.evaluation.SparqlDsl.dsl2Query
import com.signalcollect.triplerush.evaluation.SparqlDsl.{ | => | }
import org.specs2.runner.JUnitRunner
import com.signalcollect.triplerush.QueryParticle._

@RunWith(classOf[JUnitRunner])
class QueryParticleSpec extends SpecificationWithJUnit {

  sequential

  "QueryParticle" should {
    val specialInts = List(Int.MinValue, Int.MaxValue, 0, -1, 10)
    val specialLongs = specialInts.map(_.toLong) ++
      List(Long.MinValue, Long.MaxValue)

    "correctly encode the id" in {
      def testIdEncoding(queryPatternId: Int) {
        val qp = QuerySpecification(queryPatternId, Array(
          TriplePattern(-1, 1, 2),
          TriplePattern(-1, 3, -2)),
          new Array(2)).toParticle
        qp.queryId === queryPatternId
      }
      val allPassed = specialInts foreach (testIdEncoding(_))
      allPassed === true
    }

    "correctly encode the number of tickets" in {
      def testTicketEncoding(numberOftickets: Long) {
        val qp = QuerySpecification(10, Array(
          TriplePattern(-1, 1, 2),
          TriplePattern(-1, 3, -2)),
          new Array(2)).toParticle
        qp.writeTickets(numberOftickets)
        qp.tickets === numberOftickets
      }
      val allPassed = specialLongs foreach (testTicketEncoding(_))
      allPassed === true
    }

  }

}