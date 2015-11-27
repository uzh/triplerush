/*
 * Copyright (C) 2015 Cotiviti Labs (nexgen.admin@cotiviti.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.signalcollect.triplerush.dictionary

import scala.collection.JavaConversions.asScalaIterator
import org.scalatest.FlatSpec
import org.scalatest.mock.EasyMockSugar
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.{ TestStore, TripleRush }
import com.signalcollect.triplerush.sparql.{ Sparql, TripleRushGraph }
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.apache.jena.rdf.model.Model
import org.apache.jena.graph.Graph

class DictionaryErrorHandlingSpec extends FlatSpec with Checkers with EasyMockSugar {

  def withTripleStoreThatUsesDictionary(d: RdfDictionary)(test: Graph => Unit) {
    val graphBuilder = TestStore.instantiateUniqueGraphBuilder()
    val tr = TripleRush(graphBuilder, d)
    val graph = TripleRushGraph(tr)
    try {
      test(graph)
    } finally {
      graph.close()
      tr.shutdown()
      Await.result(tr.graph.system.terminate(), Duration.Inf)
    }
  }

  "TripleRush" should "propagate dictionary errors when String => ID conversion fails" in {
    val mockDictionary = mock[RdfDictionary]
    withTripleStoreThatUsesDictionary(mockDictionary) { model =>
      val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE {
  {
      <http://PersonA> foaf:name ?name .
  }
}"""
      intercept[IllegalStateException] {
        model.add(arg0)
        tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "\"Arnie\"")
      }

    }
  }

  it should "cause understandable TR failures when String => ID conversion fails" in {
    val mockDictionary = mock[RdfDictionary]
    val graphBuilder = TestStore.instantiateUniqueGraphBuilder()
    val tr = TripleRush(graphBuilder, mockDictionary)
    val graph = TripleRushGraph(tr)
    implicit val model = graph.getModel
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE {
  {
      <http://PersonA> foaf:name ?name .
  }
}"""
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "\"Arnie\"")
    val results = Sparql(sparql)
    val resultBindings = results.map(_.get("name").asLiteral.getString).toSet
    assert(resultBindings === Set("Arnie"))
    model.close()
    graph.close()
    tr.shutdown()
    Await.result(tr.graph.system.terminate(), Duration.Inf)
  }

}
