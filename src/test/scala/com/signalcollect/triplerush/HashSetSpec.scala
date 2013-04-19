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

import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner
import com.signalcollect.examples.PageRankVertex
import com.signalcollect.examples.SudokuCell
import com.signalcollect.Vertex

@RunWith(classOf[JUnitRunner])
class HashSetSpec extends SpecificationWithJUnit with Mockito {

  sequential

  "HashSet" should {

    "support adds" in {
      val hs = new HashSet(8, 0.99f)
      hs.add(1)
      hs.add(2)
      hs.add(3)
      hs.isEmpty === false
      hs.size === 3
    }

    "support array resizes" in {
      val hs = new HashSet(2, 0.99f)
      hs.add(7)
      hs.add(9)
      hs.add(3)
      hs.add(5)
      hs.add(19)
      hs.add(21)
      hs.add(15)
      hs.add(17)
      hs.isEmpty === false
      hs.size === 8
      hs.toList.sorted === List(3, 5, 7, 9, 15, 17, 19, 21)
    }

    "support removes" in {
      val hs = new HashSet(2, 0.99f)
      hs.add(7)
      hs.add(9)
      hs.add(3)
      hs.add(5)
      hs.add(19)
      hs.add(21)
      hs.add(15)
      hs.add(17)
      hs.remove(19)
      hs.remove(3)
      hs.remove(17)
      hs.remove(5)
      hs.add(17)
      hs.add(13)
      hs.isEmpty === false
      hs.size === 6
      hs.toList.sorted === List(7, 9, 13, 15, 17, 21)
    }

    "handle special keys" in {
      val hs = new HashSet(8, 0.99f)
      hs.add(1)
      hs.add(Int.MinValue)
      hs.add(Int.MaxValue)
      hs.size must_== 3
      hs.contains(1) must_== true
      hs.contains(Int.MinValue) === true
      hs.contains(Int.MaxValue) === true
    }

    "handle hash collisions correctly" in {
      case class Ab(a: Int, b: Int) {
        override def hashCode = a.hashCode
      }
      val hs = new HashSet(8, 0.99f)
      hs.add(1)
      hs.add(9)
      hs.contains(1) === true
      hs.contains(9) === true
    }

  }

}