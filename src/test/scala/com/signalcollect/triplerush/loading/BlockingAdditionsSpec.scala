/*
 *  @author Philip Stutz
 *
 *  Copyright 2015 Cotiviti
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

package com.signalcollect.triplerush.loading

import org.apache.jena.riot.Lang
import org.scalatest.FlatSpec
import org.scalatest.concurrent.ScalaFutures
import com.signalcollect.triplerush.{ GroundTruthSpec, TestStore, TriplePattern }
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class BlockingAdditionsSpec extends FlatSpec with ScalaFutures {

  "Blocking additions" should "correctly load triples from a file" in  {
    val tr = TestStore.instantiateUniqueStore()
    try {
      val resource = s"university0_0.nt"
      val tripleStream = classOf[GroundTruthSpec].getResourceAsStream(resource)
      tr.addTriples(TripleIterator(tripleStream))
      val expectedCount = 25700
      val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
      assert(count == expectedCount)
    } finally {
      tr.shutdown
      Await.result(tr.graph.system.terminate(), Duration.Inf)
    }
  }
}
