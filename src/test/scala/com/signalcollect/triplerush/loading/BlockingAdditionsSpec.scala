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

import com.signalcollect.triplerush.{GroundTruthSpec, TestStore, TriplePattern}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture.{FlatSpec, UnitFixture}

import scala.util.Try

class BlockingAdditionsSpec extends FlatSpec with ScalaFutures with UnitFixture {

  "Blocking additions" should "correctly load triples from a file" in new TestStore {
    val resource = s"university0_0.nt"
    val tripleStream = classOf[GroundTruthSpec].getResourceAsStream(resource)
    tr.addTriples(TripleIterator(tripleStream))
    val expectedCount = 25700
    val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
    assert(count == expectedCount)
  }

  it should "continue to work even after an error" in new TestStore {
    val additionAttempt = Try(tr.addTriples(new ExplodingIterator()))
    assert(additionAttempt.isFailure)
  }

}

class ExplodingIterator extends Iterator[Nothing] {

  private[this] var counter = 0

  override def hasNext: Boolean = true

  def exceptionCount() = counter

  override def next(): Nothing = {
    counter += 1
    throw new Exception("This is expected")
  }
}
