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
import org.scalatest.prop.Checkers

import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern

class EfficientIndexPatternSpec extends FlatSpec with Checkers with TestAnnouncements {

  "EfficientIndexPattern" should "correctly encode and decode index triple patterns" in {
    check((s: Int, p: Int, o: Int) => {
      val tp = TriplePattern(s, p, o)
      if ((s == 0 || p == 0 || o == 0) && s >= 0 && p >= 0 && o >= 0) {
        val efficient = tp.toEfficientIndexPattern
        val decoded = efficient.toTriplePattern
        val success = decoded === tp
        if (!success) {
          println(s"Problematic $tp was decoded as: $decoded.")
          println(s"First embedded is: ${efficient.extractFirst}")
          println(s"First mapped to positive is: ${efficient.extractFirst & Int.MaxValue}")
          println(s"Second embedded is: ${efficient.extractSecond}")
          println(s"Second mapped to positive is: ${efficient.extractSecond & Int.MaxValue}")
        }
        success
      } else {
        true
      }
    }, minSuccessful(10))
  }

}
