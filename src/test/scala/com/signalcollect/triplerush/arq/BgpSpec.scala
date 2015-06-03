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

package com.signalcollect.triplerush.arq

import scala.collection.JavaConversions.asScalaIterator

import org.scalatest.{ FlatSpec, Matchers }

import com.signalcollect.triplerush.TripleRush
import com.signalcollect.util.TestAnnouncements

class BgpSpec extends FlatSpec with Matchers with TestAnnouncements {

  "ARQ BGP" should "return the results of a simple BGP query" in {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name ?project WHERE {
  {
      ?person foaf:name ?name .
      ?person foaf:currentProject ?project
  }
}
"""
    val tr = new TripleRush
    val graph = new TripleRushGraph(tr)
    implicit val model = graph.getModel
    try {
      tr.addTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "Arnie")
      tr.addTriple("http://PersonB", "http://xmlns.com/foaf/0.1/name", "Bob")
      tr.addTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "Gardening")
      tr.addTriple("http://PersonB", "http://xmlns.com/foaf/0.1/currentProject", "Volleyball")
      tr.prepareExecution
      val results = Sparql(sparql)
      val resultBindings = results.map(_.get("project").asLiteral.getString).toSet
      assert(resultBindings === Set("Gardening", "Volleyball"))
    } finally {
      tr.shutdown
    }
  }

  it should "support BGPs with OPTIONAL, FILTER, and BOUND" in {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name ?project WHERE {
  {
      ?person foaf:name ?name .
      OPTIONAL { ?person foaf:currentProject ?project }
      FILTER(!BOUND(?project))
  }
}
"""
    val tr = new TripleRush
    val graph = new TripleRushGraph(tr)
    implicit val model = graph.getModel
    try {
      tr.addTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "Arnie")
      tr.addTriple("http://PersonB", "http://xmlns.com/foaf/0.1/name", "Bob")
      tr.addTriple("http://PersonC", "http://xmlns.com/foaf/0.1/name", "Caroline")
      tr.addTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "Gardening")
      tr.addTriple("http://PersonB", "http://xmlns.com/foaf/0.1/currentProject", "Volleyball")
      tr.prepareExecution
      val results = Sparql(sparql)
      val resultBindings = results.map(_.get("name").asLiteral.getString).toSet
      assert(resultBindings === Set("Caroline"))
    } finally {
      tr.shutdown
    }
  }

  //OPTIONAL { ?a foaf:img ?img }
  //FILTER (!bound(?member2)) .

  //  it should "return the results of a nested BGP query" in {
  //    val sparql = """
  //PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
  //SELECT ?name ?projectCount WHERE {
  //  ?person foaf:name ?name .
  //  {
  //    SELECT (COUNT(?project) as ?projectCount)
  //    {
  //      ?person foaf:currentProject ?project .
  //    }
  //    GROUP BY ?person
  //  }
  //}
  //"""
  //    //  FILTER (?projectCount > 1)
  //    val tr = new TripleRush
  //    val graph = new TripleRushGraph(tr)
  //    implicit val model = graph.getModel
  //    try {
  //      tr.addTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "Arnie")
  //      tr.addTriple("http://PersonB", "http://xmlns.com/foaf/0.1/name", "Bob")
  //      tr.addTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "Gardening")
  //      tr.addTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "Skydiving")
  //      tr.addTriple("http://PersonB", "http://xmlns.com/foaf/0.1/currentProject", "Volleyball")
  //      tr.prepareExecution
  //      val results = Sparql(sparql)
  //      val resultBindings = results.map { bindings =>
  //        println(s"person ${bindings.getLiteral("name").getString} has project count ${bindings.getLiteral("projectCount").getInt}")
  //        bindings.get("name").asLiteral.getString
  //      }.toSet
  //      assert(resultBindings === Set("Arnie"))
  //    } finally {
  //      tr.shutdown
  //    }
  //  }

}
