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

package com.signalcollect.triplerush.jena

import scala.Option.option2Iterable
import scala.collection.JavaConversions.asScalaIterator
import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.UnitFixture
import com.signalcollect.triplerush.TestStore
import com.signalcollect.triplerush.BlankNodeNamespace
import org.apache.jena.graph.NodeFactory
import org.apache.jena.graph.{ Triple => JenaTriple }

class BlankNodeSpec extends FlatSpec with UnitFixture {

  "Blank nodes" should "have small assigned TR blank node IDs when using a blank node namespace"  in new TestStore {
    val ns = new BlankNodeNamespace
    val bn1 = NodeFactory.createBlankNode()
    val bn2 = NodeFactory.createBlankNode()
    val bn3 = NodeFactory.createBlankNode()
    val triple = new JenaTriple(bn1, bn2, bn3)
    tr.addTriples(Iterator.single(triple), Some(ns))
    assert(ns.mappings.values.toSet == Set(1, 2, 3))
    assert(tr.dictionary.isBlankNodeId(1))
    assert(tr.dictionary.isBlankNodeId(2))
    assert(tr.dictionary.isBlankNodeId(3))
  }

}
