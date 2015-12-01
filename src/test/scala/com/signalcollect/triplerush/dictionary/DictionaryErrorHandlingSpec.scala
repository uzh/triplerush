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
import com.signalcollect.triplerush.sparql.Sparql

class DictionaryErrorHandlingSpec extends FlatSpec with Checkers with EasyMockSugar {

  "TripleRush" should "propagate dictionary errors when String => ID conversion fails" in {
    val mockDictionary = mock[RdfDictionary]
    val graphBuilder = TestStore.instantiateUniqueGraphBuilder()
    val tr = TripleRush(graphBuilder, mockDictionary)
    implicit val model = tr.getModel
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE {
  {
      <http://PersonA> foaf:name ?name .
  }
}"""
    intercept[IllegalStateException] {
      tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "\"Arnie\"")
    }
  }

}
