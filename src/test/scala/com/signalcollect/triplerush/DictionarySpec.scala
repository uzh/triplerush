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

import org.scalatest.FlatSpec

class DictonarySpec extends FlatSpec with TestAnnouncements {

  "Dictionary" should "support adding entries in parallel" in {
    val d = new HashMapDictionary(1, 0.2f)
    val stringEntries = (1 to 1000).map(_.toString)
    for (entry <- stringEntries.par) {
      d(entry)
    }
    val reverseMapped = (1 to 1000).map(d(_)).toSet
    assert(reverseMapped.size == 1000)
    assert(stringEntries.toSet == reverseMapped.toSet)
  }

  "Dictionary" should "support clear" in {
    val d = new HashMapDictionary(1, 0.2f)
    val lowStringEntries = (1 to 1000).map(_.toString)
    for (entry <- lowStringEntries.par) {
      d(entry)
    }
    d.clear
    val highStringEntries = (1001 to 2000).map(_.toString)
    for (entry <- highStringEntries.par) {
      d(entry)
    }
    val reverseMapped = (1 to 1000).map(d(_)).toSet
    assert(reverseMapped.size == 1000)
    assert(reverseMapped.map(_.toInt).min == 1001)
    assert(highStringEntries.toSet == reverseMapped.toSet)
  }

}
