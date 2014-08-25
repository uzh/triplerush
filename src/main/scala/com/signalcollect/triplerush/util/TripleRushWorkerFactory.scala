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

import scala.reflect.ClassTag

import com.signalcollect.interfaces.EdgeAddedToNonExistentVertexHandlerFactory
import com.signalcollect.interfaces.ExistingVertexHandlerFactory
import com.signalcollect.interfaces.MapperFactory
import com.signalcollect.interfaces.MessageBusFactory
import com.signalcollect.interfaces.SchedulerFactory
import com.signalcollect.interfaces.StorageFactory
import com.signalcollect.interfaces.UndeliverableSignalHandlerFactory
import com.signalcollect.interfaces.WorkerFactory
import com.signalcollect.worker.AkkaWorker

class TripleRushWorkerFactory[Signal: ClassTag] extends WorkerFactory[Long, Signal] {
  def createInstance(
    workerId: Int,
    numberOfWorkers: Int,
    numberOfNodes: Int,
    messageBusFactory: MessageBusFactory[Long, Signal],
    mapperFactory: MapperFactory[Long],
    storageFactory: StorageFactory[Long, Signal],
    schedulerFactory: SchedulerFactory[Long, Signal],
    existingVertexHandlerFactory: ExistingVertexHandlerFactory[Long, Signal],
    undeliverableSignalHandlerFactory: UndeliverableSignalHandlerFactory[Long, Signal],
    edgeAddedToNonExistentVertexHandlerFactory: EdgeAddedToNonExistentVertexHandlerFactory[Long, Signal],
    heartbeatIntervalInMilliseconds: Int,
    eagerIdleDetection: Boolean,
    throttlingEnabled: Boolean,
    throttlingDuringLoadingEnabled: Boolean,
    supportBlockingGraphModificationsInVertex: Boolean): AkkaWorker[Long, Signal] = {
    new TripleRushWorker[Signal](
      workerId,
      numberOfWorkers,
      numberOfNodes,
      messageBusFactory,
      mapperFactory,
      storageFactory,
      schedulerFactory,
      existingVertexHandlerFactory,
      undeliverableSignalHandlerFactory,
      edgeAddedToNonExistentVertexHandlerFactory,
      heartbeatIntervalInMilliseconds,
      eagerIdleDetection,
      throttlingEnabled,
      throttlingDuringLoadingEnabled,
      supportBlockingGraphModificationsInVertex)
  }
  override def toString: String = "TripleRushWorkerFactory"
}
