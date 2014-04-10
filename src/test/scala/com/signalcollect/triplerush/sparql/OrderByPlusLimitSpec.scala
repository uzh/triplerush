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
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.ResourceFactory
import collection.JavaConversions._

class OrderByPlusLimitSpec extends FlatSpec with Matchers with TestAnnouncements {

  "ORDER BY and LIMIT" should "return the same results as Jena for IRIs" in {
    val sparql = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
}
ORDER BY ?label
LIMIT 3
"""
    implicit val tr = new TripleRush
    try {
      tr.addTriple("http://SomeProduct2", "http://www.w3.org/2000/01/rdf-schema#label", "http://B")
      tr.addTriple("http://SomeProduct3", "http://www.w3.org/2000/01/rdf-schema#label", "http://C")
      tr.addTriple("http://SomeProduct4", "http://www.w3.org/2000/01/rdf-schema#label", "http://D")
      tr.addTriple("http://SomeProduct5", "http://www.w3.org/2000/01/rdf-schema#label", "http://E")
      tr.addTriple("http://SomeProduct1", "http://www.w3.org/2000/01/rdf-schema#label", "http://A")
      tr.prepareExecution
      val query = Sparql(sparql).get
      //      val jena = ModelFactory.createDefaultModel
      //      val label = ResourceFactory.createProperty("http://www.w3.org/2000/01/rdf-schema#label")
      //      val p1 = ResourceFactory.createResource("http://SomeProduct1")
      //      val p2 = ResourceFactory.createResource("http://SomeProduct2")
      //      val p3 = ResourceFactory.createResource("http://SomeProduct3")
      //      val p4 = ResourceFactory.createResource("http://SomeProduct4")
      //      val p5 = ResourceFactory.createResource("http://SomeProduct5")
      //      val a = ResourceFactory.createResource("http://A")
      //      val b = ResourceFactory.createResource("http://B")
      //      val c = ResourceFactory.createResource("http://C")
      //      val d = ResourceFactory.createResource("http://D")
      //      val e = ResourceFactory.createResource("http://E")
      //      jena.add(ResourceFactory.createStatement(p1, label, a))
      //      jena.add(ResourceFactory.createStatement(p2, label, b))
      //      jena.add(ResourceFactory.createStatement(p3, label, c))
      //      jena.add(ResourceFactory.createStatement(p4, label, d))
      //      jena.add(ResourceFactory.createStatement(p5, label, e))
      //      val jenaQuery = QueryFactory.create(sparql)
      //      val jenaQueryExecution = QueryExecutionFactory.create(jenaQuery, jena)
      //      val jenaResultIterator = jenaQueryExecution.execSelect
      //      println(jenaResultIterator.toList.map(_.get("label")))
      val result = query.resultIterator.map(bindings => (bindings("label"), bindings("product"))).toList
      assert(result === List(("http://A", "http://SomeProduct1"), ("http://B", "http://SomeProduct2"), ("http://C", "http://SomeProduct3")))
    } finally {
      tr.shutdown
    }
  }

}
