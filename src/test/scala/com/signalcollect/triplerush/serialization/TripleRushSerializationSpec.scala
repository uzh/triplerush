/*
 *  @author Philip Stutz
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

package com.signalcollect.triplerush.serialization

import org.scalatest.{ FlatSpec, Matchers }

import com.signalcollect.GraphBuilder
import com.signalcollect.triplerush.{TestConfig, TriplePattern, TripleRush}
import com.signalcollect.util.TestAnnouncements

class TripleRushSerializationSpec extends FlatSpec with Matchers with TestAnnouncements {

  "TripleRush serialization" should "support full message serialization" in {
    val sparql = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
}
ORDER BY ?label
LIMIT 3
                 """
    val config = TestConfig.system()
    implicit val tr = TripleRush(
      graphBuilder = new GraphBuilder[Long, Any].withMessageSerialization(true), config = config)
    try {
      tr.addTriplePattern(TriplePattern(1, 2, 3))
      tr.addTriplePattern(TriplePattern(3, 2, 5))
      tr.addTriplePattern(TriplePattern(3, 2, 7))
      tr.prepareExecution
      tr.resultIteratorForQuery(Seq(TriplePattern(1, 2, -1), TriplePattern(-1, 2, -2)))
      Thread.sleep(1000)
    } finally {
      tr.shutdown
    }
  }

}
