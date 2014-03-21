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

import com.signalcollect.util.IntHashMap
import com.signalcollect.util.IntValueHashMap
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

object Dictionary {
  private val lock = new ReentrantReadWriteLock
  private val read = lock.readLock
  private val write = lock.writeLock
  private var id2String = new IntHashMap[String]
  private var string2Id = new IntValueHashMap[String]
  private var maxId = 0

  def contains(s: String): Boolean = {
    read.lock
    try {
      val hasExistingEncoding = string2Id.get(s) != 0
      hasExistingEncoding
    } finally {
      read.unlock
    }
  }

  def apply(s: String): Int = {
    read.lock
    val existingEncoding: Int = try {
      string2Id.get(s)
    } finally {
      read.unlock
    }
    if (existingEncoding == 0) {
      write.lock
      try {
        val id = {
          maxId += 1
          maxId
        }
        string2Id.put(s, id)
        id2String.put(id, s)
        id
      } finally {
        write.unlock
      }
    } else {
      existingEncoding
    }
  }

  def apply(id: Int): String = {
    read.lock
    try {
      id2String.get(id)
    } finally {
      read.unlock
    }
  }

  def decode(id: Int): Option[String] = {
    val decoded = apply(id)
    if (decoded != null) {
      Some(decoded)
    } else {
      None
    }
  }

}
