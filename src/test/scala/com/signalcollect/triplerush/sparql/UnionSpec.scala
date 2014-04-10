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

package com.signalcollect.triplerush.sparql

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.signalcollect.triplerush.TestAnnouncements
import com.signalcollect.triplerush.TripleRush
import scala.concurrent.Await
import scala.concurrent.duration._

class UnionSpec extends FlatSpec with Matchers with TestAnnouncements {

  "UNION" should "return the results of two separate queries" in {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE {
  {
      <http://SomePerson> foaf:name ?name .
  } UNION {
      <http://ThatGuy> foaf:name ?name
  }
}
"""
    implicit val tr = new TripleRush
    try {
      tr.addTriple("http://SomePerson", "http://xmlns.com/foaf/0.1/name", "Harold")
      tr.addTriple("http://SomeOtherPerson", "http://xmlns.com/foaf/0.1/name", "Harold")
      tr.addTriple("http://ThatGuy", "http://xmlns.com/foaf/0.1/name", "Arthur")
      tr.prepareExecution
      val query = Sparql(sparql).get
      val result = query.resultIterator.map(_("name")).toSet
      assert(result === Set("Harold", "Arthur"))
    } finally {
      tr.shutdown
    }
  }

}
