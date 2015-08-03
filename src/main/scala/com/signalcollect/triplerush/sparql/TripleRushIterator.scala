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

import com.hp.hpl.jena.graph.{ NodeFactory, Triple }
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory
import com.hp.hpl.jena.datatypes.TypeMapper
import com.signalcollect.triplerush.dictionary.Dictionary

object TripleRushIterator {

  /**
   * Assumes queries of the form `?s ?p ?o`, where in the place of each variable there could also be an IRI or literal.
   */
  def convert(
    sOption: Option[String],
    pOption: Option[String],
    oOption: Option[String],
    dictionary: Dictionary,
    i: Iterator[Array[Int]]): Iterator[Triple] = {
    val sNode = sOption.map(NodeConversion.stringToNode(_))
    val pNode = pOption.map(NodeConversion.stringToNode(_))
    val oNode = oOption.map(NodeConversion.stringToNode(_))

    @inline def toTriple(r: Array[Int]): Triple = {
      var i = 0
      val s = sNode.getOrElse {
        val n = NodeConversion.stringToNode(dictionary.unsafeDecode(r(i)))
        i += 1
        n
      }
      val p = pNode.getOrElse {
        val n = NodeConversion.stringToNode(dictionary.unsafeDecode(r(i)))
        i += 1
        n
      }
      val o = oNode.getOrElse {
        NodeConversion.stringToNode(dictionary.unsafeDecode(r(i)))
      }
      new Triple(s, p, o)
    }

    i.map(toTriple(_))
  }

}
