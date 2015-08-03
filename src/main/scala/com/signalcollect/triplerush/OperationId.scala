/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *
 *  Copyright 2013 University of Zurich
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

import java.util.concurrent.atomic.AtomicInteger

import scala.language.implicitConversions

import com.signalcollect.triplerush.EfficientIndexPattern._
import com.signalcollect.triplerush.sparql.VariableEncoding

object OperationIds {
  private[this] val maxOperationId = new AtomicInteger(0)
  private[this] val minOperationId = new AtomicInteger(0)

  def nextId: Int = {
    maxOperationId.incrementAndGet
  }

  def nextCountQueryId: Int = {
    minOperationId.decrementAndGet
  }

  def reset {
    maxOperationId.set(0)
    minOperationId.set(0)
  }

  // Int.MinValue cannot be embedded
  @inline def embedInLong(operationId: Int): Long = {
    assert(operationId != Int.MinValue)
    if (operationId < 0) {
      EfficientIndexPattern.embed2IntsInALong(operationId, Int.MinValue)
    } else {
      EfficientIndexPattern.embed2IntsInALong(Int.MinValue, operationId | Int.MinValue)
    }
  }

  @inline def extractFromLong(efficientIndexId: Long): Int = {
    val first = efficientIndexId.extractFirst
    if (first == Int.MinValue) {
      efficientIndexId.extractSecond & Int.MaxValue
    } else {
      first
    }
  }

}
