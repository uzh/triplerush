/*
 *  @author Philip Stutz
 *
 *  Copyright 2015 iHealth Technologies
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

import com.signalcollect.GraphBuilder
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.fixture.NoArg

object TestStore {

  val nextUniquePrefix = new AtomicInteger(0)

  def instantiateUniqueStore(fastStart: Boolean): TripleRush = {
    val uniquePrefix = nextUniquePrefix.incrementAndGet.toString
    val graphBuilder = new GraphBuilder[Long, Any]().withActorNamePrefix(uniquePrefix)
    TripleRush(graphBuilder = graphBuilder, fastStart = fastStart)
  }

}

class TestStore(val tr: TripleRush) extends NoArg {

  def this() = this(
    TestStore.instantiateUniqueStore(fastStart = true))

  def shutdown(): Unit = tr.shutdown()

  override def apply(): Unit = {
    try super.apply()
    finally shutdown()
  }

}
