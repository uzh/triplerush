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

object Dictionary {
  private var id2String = new IntHashMap[String]
  private var string2Id = new IntValueHashMap[String]
  private var maxId = 0

  def contains(s: String): Boolean = synchronized {
    val existingEncoding = string2Id.get(s)
    existingEncoding != 0
  }

  def apply(s: String): Int = synchronized {
    val existingEncoding = string2Id.get(s)
    if (existingEncoding == 0) {
      val id = {
        maxId += 1
        maxId
      }
      string2Id.put(s, id)
      id2String.put(id, s)
      id
    } else {
      existingEncoding
    }
  }

  def apply(id: Int): Option[String] = synchronized {
    val decoded = id2String.get(id)
    if (decoded != 0) {
      Some(decoded)
    } else {
      None
    }
  }

}
