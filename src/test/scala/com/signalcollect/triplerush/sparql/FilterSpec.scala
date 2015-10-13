/*
 *  @author Philip Stutz
 *
 *  Copyright 2015 iHealth Technologies
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

package com.signalcollect.triplerush.sparql

import org.scalatest.{ Finders, FlatSpec, Matchers }
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.util.TestAnnouncements
import com.signalcollect.triplerush.TestStore

class FilterSpec extends FlatSpec with Matchers with TestAnnouncements {

  "ARQ FILTER" should "return the correct result even when all variables are bound during an existence check" in new TestStore {
    val sparql = """
SELECT ?r {
  ?r <http://p> <http://r2>
  FILTER EXISTS { ?r <http://p> <http://r3> }
}"""
    tr.addStringTriple("http://r1", "http://p", "r2")
        val results = Sparql(sparql)
    assert(!results.hasNext)
  }

}
