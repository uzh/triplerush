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

import com.hp.hpl.jena.graph.{ NodeFactory, Triple }
import com.signalcollect.triplerush.Dictionary

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
    val sNode = sOption.map(NodeFactory.createURI(_))
    val pNode = pOption.map(NodeFactory.createURI(_))
    val oNode = {
      oOption.map { o =>
        if (o.startsWith("http://")) {
          NodeFactory.createURI(o)
        } else {
          NodeFactory.createLiteral(o)
        }
      }
    }
    @inline def toTriple(r: Array[Int]): Triple = {
      var i = 0
      val s = sNode.getOrElse {
        val n = NodeFactory.createURI(dictionary.unsafeDecode(r(i)))
        i += 1
        n
      }
      val p = pNode.getOrElse {
        val n = NodeFactory.createURI(dictionary.unsafeDecode(r(i)))
        i += 1
        n
      }
      val o = oNode.getOrElse {
        val decoded = dictionary.unsafeDecode(r(i))
        val n = if (decoded.startsWith("http://")) {
          NodeFactory.createURI(decoded)
        } else {
          NodeFactory.createLiteral(decoded)
        }
        n
      }
      new Triple(s, p, o)
    }
    i.map(toTriple(_))
  }

}
