/*
 *  @author Philip Stutz
 *
 *  Copyright 2012 University of Zurich
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

package com.signalcollect.triplerush.util

import com.signalcollect.interfaces._
import com.signalcollect.worker.AkkaWorker
import com.signalcollect.worker.WorkerImplementation
import scala.reflect.ClassTag
import com.signalcollect._

final class TripleRushWorker[Signal: ClassTag](
  workerId: Int,
  numberOfWorkers: Int,
  numberOfNodes: Int,
  messageBusFactory: MessageBusFactory[Long, Signal],
  mapperFactory: MapperFactory[Long],
  storageFactory: StorageFactory[Long],
  schedulerFactory: SchedulerFactory[Long],
  heartbeatIntervalInMilliseconds: Int,
  eagerIdleDetection: Boolean,
  throttlingEnabled: Boolean) extends AkkaWorker[Long, Signal](
  workerId,
  numberOfWorkers,
  numberOfNodes,
  messageBusFactory,
  mapperFactory,
  storageFactory,
  schedulerFactory,
  heartbeatIntervalInMilliseconds,
  eagerIdleDetection,
  throttlingEnabled) {

  override val worker = new WorkerImplementation[Long, Signal](
    workerId = workerId,
    numberOfWorkers = numberOfWorkers,
    numberOfNodes = numberOfNodes,
    eagerIdleDetection = eagerIdleDetection,
    messageBus = messageBus,
    log = log,
    storageFactory = storageFactory,
    schedulerFactory = schedulerFactory,
    signalThreshold = 0.01,
    collectThreshold = 0.0,
    existingVertexHandler = (vOld, vNew, ge) => (),
    undeliverableSignalHandler = (s: Signal, tId: Long, sId: Option[Long], ge: GraphEditor[Long, Signal]) => {
      throw new Exception(s"Undeliverable signal: $s from $sId could not be delivered to $tId.")
      Unit
    },
    edgeAddedToNonExistentVertexHandler = (edge: Edge[Long], vertexId: Long) => {
      throw new Exception(
        s"Could not add edge: ${edge.getClass.getSimpleName}(id = $vertexId -> ${edge.targetId}), because vertex with id $vertexId does not exist.")
      None
    })

}
  