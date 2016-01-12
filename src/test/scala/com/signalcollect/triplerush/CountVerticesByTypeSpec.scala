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

package com.signalcollect.triplerush

import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.UnitFixture

class CountVerticesByTypeSpec extends FlatSpec with UnitFixture {

  "Count vertices by type" should "work on an empty triple store" in new TestStore {
    val iri = "http://abc"
    assert(tr.countVerticesByType.size == 1, "An empty store should only contain the root vertex.")
  }

  it should "return each index once after one triple was added" in new TestStore {
    val iri = "http://abc"
    tr.addStringTriples(Iterator.single((iri, iri, iri)))
    val vertexTypeMap = tr.countVerticesByType
    assert(vertexTypeMap.size == IndexType.list.size, "Each index vertex type should have one map entry.")
    assert(vertexTypeMap.values.forall(_ == 1), "There should be exactly one instance of each vertex type.")
  }

}
