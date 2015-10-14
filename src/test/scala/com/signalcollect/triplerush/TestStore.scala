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
import org.apache.jena.graph.Graph
import org.apache.jena.rdf.model.Model
import com.signalcollect.triplerush.sparql.TripleRushGraph

object TestStore {

  val nextUniquePrefix = new AtomicInteger(0)

  def instantiateUniqueStore(): TripleRush = {
    val uniquePrefix = nextUniquePrefix.incrementAndGet.toString
    val graphBuilder = new GraphBuilder[Long, Any]().withActorNamePrefix(uniquePrefix)
    TripleRush(graphBuilder = graphBuilder)
  }

}

class TestStore(val tr: TripleRush) extends NoArg {

  lazy implicit val graph = TripleRushGraph(tr)
  lazy implicit val model = graph.getModel

  def this() = this(TestStore.instantiateUniqueStore())

  def shutdown(): Unit = {
    model.close()
    graph.close()
    tr.shutdown()
  }

  override def apply(): Unit = {
    try super.apply()
    finally shutdown()
  }

}
