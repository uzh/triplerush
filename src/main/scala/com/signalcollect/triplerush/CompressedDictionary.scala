/*
 *  @author Philip Stutz
 *  
 *  Copyright 2014 University of Zurich
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

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.signalcollect.triplerush.util.CompositeLongIntHashMap
import com.signalcollect.util.IntLongHashMap

class CompressedDictionary extends Dictionary {

  val idToPrefixedId = new IntLongHashMap(initialSize = 128, rehashFraction = 0.4f)
  val prefixedIdToId = new CompositeLongIntHashMap(initialSize = 128, rehashFraction = 0.4f)
  val impl = new HashMapDictionary

  private val lock = new ReentrantReadWriteLock
  private val read = lock.readLock
  private val write = lock.writeLock

  /**
   * Search for the last useful occurrence of a slash, then check if a hash still appears after that.
   */
  @inline final def getSplitIndex(s: String): Int = {
    val slashSearchStartIndex = s.length - 2
    val slashIndex = s.lastIndexOf('/', slashSearchStartIndex)
    val hashIndex = s.indexOf('#', slashIndex)
    math.max(slashIndex, hashIndex)
  }

  def contains(s: String): Boolean = {
    val splitIndex = getSplitIndex(s)
    if (splitIndex > 0 && splitIndex < (s.length - 1)) {
      val prefix = s.substring(0, splitIndex + 1)
      val remainder = s.substring(splitIndex + 1)
      val prefixId = impl.apply(prefix)
      if (prefixId > 0) {
        val remainderId = impl.apply(remainder)
        if (remainderId > 0) {
          val compositeLongRepresentation = EfficientIndexPattern.embed2IntsInALong(prefixId, remainderId)
          val potentiallyExistingCompositeId = prefixedIdToId.get(compositeLongRepresentation)
          potentiallyExistingCompositeId > 0
        } else {
          false
        }
      } else {
        false
      }
    } else {
      impl.contains(s)
    }
  }

  def apply(s: String): Int = {
    val splitIndex = getSplitIndex(s)
    if (splitIndex > 0 && splitIndex < (s.length - 1)) {
      val prefix = s.substring(0, splitIndex + 1)
      val remainder = s.substring(splitIndex + 1)
      val prefixId = impl.apply(prefix)
      val remainderId = impl.apply(remainder)
      val compositeLongRepresentation = EfficientIndexPattern.embed2IntsInALong(prefixId, remainderId)
      write.lock
      try {
        val potentiallyExistingCompositeId = prefixedIdToId.get(compositeLongRepresentation)
        if (potentiallyExistingCompositeId > 0) {
          potentiallyExistingCompositeId
        } else {
          val compositeId = impl.reserveId
          idToPrefixedId.put(compositeId, compositeLongRepresentation)
          prefixedIdToId.put(compositeLongRepresentation, compositeId)
          compositeId
        }
      } finally {
        write.unlock
      }
    } else {
      impl.apply(s)
    }
  }

  @inline final def unsafeGetEncoded(s: String): Int = {
    val splitIndex = getSplitIndex(s)
    if (splitIndex > 0 && splitIndex < (s.length - 1)) {
      val prefix = s.substring(0, splitIndex + 1)
      val remainder = s.substring(splitIndex + 1)
      val prefixId = impl.unsafeGetEncoded(prefix)
      if (prefixId > 0) {
        val remainderId = impl.unsafeGetEncoded(remainder)
        if (remainderId > 0) {
          val compositeLongRepresentation = EfficientIndexPattern.embed2IntsInALong(prefixId, remainderId)
          val potentiallyExistingCompositeId = prefixedIdToId.get(compositeLongRepresentation)
          potentiallyExistingCompositeId
        } else {
          0
        }
      } else {
        0
      }
    } else {
      impl.unsafeGetEncoded(s)
    }
  }

  def apply(id: Int): String = {
    read.lock
    try {
      val prefixedId = idToPrefixedId.get(id)
      if (prefixedId != 0) {
        val prefixId = new EfficientIndexPattern(prefixedId).extractFirst
        val remainderId = new EfficientIndexPattern(prefixedId).extractSecond
        impl.apply(prefixId) + impl.apply(remainderId)
      } else {
        impl.apply(id)
      }
    } finally {
      read.unlock
    }
  }

  // Can only be called, when there are no concurrent writes.
  @inline final def unsafeDecode(id: Int): String = {
    val prefixedId = idToPrefixedId.get(id)
    if (prefixedId != 0) {
      val prefixId = new EfficientIndexPattern(prefixedId).extractFirst
      val remainderId = new EfficientIndexPattern(prefixedId).extractSecond
      impl.apply(prefixId) + impl.apply(remainderId)
    } else {
      impl.apply(id)
    }
  }

  def decode(id: Int): Option[String] = {
    read.lock
    try {
      val prefixedId = idToPrefixedId.get(id)
      if (prefixedId != 0) {
        val prefixId = new EfficientIndexPattern(prefixedId).extractFirst
        val remainderId = new EfficientIndexPattern(prefixedId).extractSecond
        Some(impl.apply(prefixId) + impl.apply(remainderId))
      } else {
        val decoded = impl.apply(id)
        if (decoded != null) {
          Some(decoded)
        } else {
          None
        }
      }
    } finally {
      read.unlock
    }
  }

  def clear = {
    write.lock
    try {
      impl.clear
      idToPrefixedId.clear
      prefixedIdToId.clear
    } finally {
      write.unlock
    }
  }

}
