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

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.signalcollect.triplerush.TripleRush
import scala.concurrent.Await
import scala.concurrent.duration._
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.rdf.model.ResourceFactory
import collection.JavaConversions._
import com.signalcollect.util.TestAnnouncements

class OrderByPlusLimitSpec extends FlatSpec with Matchers with TestAnnouncements {

  "ARQ ORDER BY and LIMIT" should "return the same results as Jena for IRIs" in {
    val sparql = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
}
ORDER BY ?label
LIMIT 3
                 """
    val tr = new TripleRush
    val graph = new TripleRushGraph(tr)
    implicit val model = graph.getModel
    try {
      tr.addStringTriple("http://SomeProduct2", "http://www.w3.org/2000/01/rdf-schema#label", "http://B")
      tr.addStringTriple("http://SomeProduct3", "http://www.w3.org/2000/01/rdf-schema#label", "http://C")
      tr.addStringTriple("http://SomeProduct4", "http://www.w3.org/2000/01/rdf-schema#label", "http://D")
      tr.addStringTriple("http://SomeProduct5", "http://www.w3.org/2000/01/rdf-schema#label", "http://E")
      tr.addStringTriple("http://SomeProduct1", "http://www.w3.org/2000/01/rdf-schema#label", "http://A")
      tr.prepareExecution
      val results = Sparql(sparql)
      val bindingsList = results.toList.map(bindings =>
        (bindings.get("label").asResource.toString, bindings.get("product").asResource.toString)).toList
      assert(bindingsList === List(("http://A", "http://SomeProduct1"), ("http://B", "http://SomeProduct2"), ("http://C", "http://SomeProduct3")))
    } finally {
      tr.shutdown
    }
  }

}
