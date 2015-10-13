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

package com.signalcollect.triplerush

import org.apache.jena.graph.{ Triple => JenaTriple }
import scala.concurrent.Await
import scala.concurrent.duration.Duration

trait BlockingOperations {
  this: TripleRush =>

  import TripleRush.defaultBlockingOperationTimeout

  def loadFromIterator(
    iterator: Iterator[JenaTriple],
    placementHint: Option[Long] = Some(OperationIds.embedInLong(OperationIds.nextId))): Unit = {
    asyncLoadFromIterator(iterator, placementHint)
    graph.awaitIdle
  }

  def addTriplePatterns(i: Iterator[TriplePattern], timeout: Duration = defaultBlockingOperationTimeout): Unit = {
    Await.result(asyncAddTriplePatterns(i), timeout)
  }

  def addEncodedTriple(sId: Int, pId: Int, oId: Int, timeout: Duration = defaultBlockingOperationTimeout): Unit = {
    Await.result(asyncAddEncodedTriple(sId, pId, oId), timeout)
  }

  def count(q: Seq[TriplePattern], timeout: Duration = defaultBlockingOperationTimeout): Long = {
    Await.result(asyncCount(q), timeout)
      .getOrElse(throw new Exception("Insufficient tickets to completely execute counting query."))
  }

  def getIndexAt(indexId: Long, timeout: Duration = defaultBlockingOperationTimeout): Array[Int] = {
    Await.result(asyncGetIndexAt(indexId), timeout)
  }
  
}
