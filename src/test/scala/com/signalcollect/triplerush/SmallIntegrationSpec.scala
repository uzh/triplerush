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
import com.signalcollect.triplerush.evaluation.SparqlDsl._
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SmallIntegrationSpec extends SpecificationWithJUnit {

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
      result.length === 2
    }

  }

  val qe = new QueryEngine(graphBuilder = GraphBuilder.
    withMessageBusFactory(new BulkAkkaMessageBusFactory(1024, false)).
    withMessageSerialization(true))

  println("Loading small dataset ... ")
  qe.loadTriple("A", "takesCourse", "Department0.University0.GraduateCourse0")
  qe.loadTriple("A", "type", "GraduateStudent")
  qe.loadTriple("B", "takesCourse", "Department0.University0.GraduateCourse0")
  qe.loadTriple("B", "type", "GraduateStudent")
  qe.loadTriple("C", "takesCourse", "Department0.University0.GraduateCourse1")
  qe.loadTriple("C", "type", "GraduateStudent")
  qe.awaitIdle
  println("Finished loading.")

  print("Optimizing edge representations...")
  qe.prepareQueryExecution
  println("done")

  def executeOnQueryEngine(q: QuerySpecification) = {
    val resultFuture = qe.executeQuery(q)
    val result = Await.result(resultFuture, new FiniteDuration(100, TimeUnit.SECONDS))
    result.queries
  }

}