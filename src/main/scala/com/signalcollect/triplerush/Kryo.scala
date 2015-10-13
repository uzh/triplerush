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

import com.signalcollect.interfaces._
import com.signalcollect.triplerush.handlers._
import com.signalcollect.triplerush.mapper._
import com.signalcollect.triplerush.util._
import com.signalcollect.triplerush.vertices._
import com.signalcollect.triplerush.vertices.query._

object Kryo {

  val defaultRegistrations: List[String] = {
    List(
      classOf[RootIndex].getName,
      classOf[SIndex].getName,
      classOf[PIndex].getName,
      classOf[OIndex].getName,
      classOf[SPIndex].getName,
      classOf[POIndex].getName,
      classOf[SOIndex].getName,
      classOf[TriplePattern].getName,
      classOf[IndexVertexEdge].getName,
      classOf[BlockingIndexVertexEdge].getName,
      classOf[CardinalityRequest].getName,
      classOf[CardinalityReply].getName,
      classOf[PredicateStatsReply].getName,
      classOf[ChildIdRequest].getName,
      classOf[ChildIdReply].getName,
      classOf[SubjectCountSignal].getName,
      classOf[ObjectCountSignal].getName,
      classOf[TriplePattern].getName,
      classOf[PredicateStats].getName,
      classOf[ResultIteratorQueryVertex].getName,
      classOf[ResultIterator].getName,
      classOf[TripleRushWorkerFactory[Any]].getName,
      TripleRushEdgeAddedToNonExistentVertexHandlerFactory.getClass.getName,
      TripleRushUndeliverableSignalHandlerFactory.getClass.getName,
      TripleRushStorage.getClass.getName,
      SingleNodeTripleMapperFactory.getClass.getName,
      new AlternativeTripleMapperFactory(false).getClass.getName,
      DistributedTripleMapperFactory.getClass.getName,
      RelievedNodeZeroTripleMapperFactory.getClass.getName,
      LoadBalancingTripleMapperFactory.getClass.getName,
      classOf[CombiningMessageBusFactory[_]].getName,
      classOf[AddEdge[Any, Any]].getName,
      classOf[AddEdge[Long, Long]].getName) // TODO: Can we force the use of the specialized version?)
  }

}
