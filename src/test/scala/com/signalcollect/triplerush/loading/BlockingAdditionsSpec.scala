/*
 * Copyright (C) 2015 Cotiviti Labs (nexgen.admin@cotiviti.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.signalcollect.triplerush.loading

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Try

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture.{ FlatSpec, UnitFixture }

import com.signalcollect.triplerush.{ GroundTruthSpec, TestStore, TriplePattern }

class BlockingAdditionsSpec extends FlatSpec with ScalaFutures with UnitFixture {

  val resource = s"university0_0.nt"
  def tripleStream = classOf[GroundTruthSpec].getResourceAsStream(resource)

  "Blocking additions" should "correctly load triples from a file" in new TestStore {
    tr.addTriples(TripleIterator(tripleStream))
    val expectedCount = 25700
    val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
    assert(count == expectedCount)
  }

  it should "continue to work even after an error" in new TestStore {
    val additionAttempt = Try(tr.addTriples(new ExplodingIterator()))
    assert(additionAttempt.isFailure)
  }

  it should "be able to load a lot of redundant triples into limited memory" in new TestStore {
    val batches = 10
    val parallelAdditions = 10
    (1 to batches) foreach { i =>
      val f = Future.sequence {
        (1 to parallelAdditions) map { i =>
          Future {
            tr.addTriples(TripleIterator(tripleStream))
          }
        }
      }
      Await.result(f, 300.seconds)
//      println(s"$i/$batches batches")
      val expectedCount = 25700
      val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
//      println(tr.dictionary)
      assert(count == expectedCount)
    }
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
