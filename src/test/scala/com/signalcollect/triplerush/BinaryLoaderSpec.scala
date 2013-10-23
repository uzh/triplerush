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
import com.signalcollect.triplerush.evaluation.SparqlDsl.{| => |}
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BinaryLoaderSpec extends SpecificationWithJUnit {

  sequential

  val dslQueries = List(
    // Query 1
    SELECT ? "X" WHERE (
      | - "X" - "takesCourse" - "Department0.University0.GraduateCourse0",
      | - "X" - "type" - s"GraduateStudent"))

  "Small query 1" should {
    val queryId = 1
    s"DSL-match the reference results $queryId" in {
      val query = dslQueries(queryId - 1)
      val result = executeOnQueryEngine(query)
      result.length === 0
    }

  }

  val qe = new QueryEngine(graphBuilder = GraphBuilder.
    withMessageBusFactory(new BulkAkkaMessageBusFactory(1024, false))
    .withConsole(true, 8080))

  println("Loading small dataset ... ")
  qe.loadBinary("./binary-split/0.filtered-split", Some(0))
  qe.awaitIdle
  println("Finished loading.")

  def executeOnQueryEngine(q: QuerySpecification) = {
    val resultFuture = qe.executeQuery(q)
    val result = Await.result(resultFuture, new FiniteDuration(100, TimeUnit.SECONDS))
    result.bindings
  }

}