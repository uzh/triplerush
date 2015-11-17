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

package com.signalcollect.triplerush.sparql

import org.apache.jena.graph.Node
import org.apache.jena.graph.impl.LiteralLabelFactory
import org.apache.jena.graph.NodeFactory
import org.apache.jena.rdf.model.AnonId
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.BlankNodeId

object NodeConversion {

  // TODO: Test combination of tag with explicit string type, ensure type is dropped.
  def nodeToString(n: Node): String = {
    val nodeAsString = if (n.isURI) {
      n.toString(null, false)
    } else if (n.isLiteral) {
      n.getLiteralValue match {
        case i: Integer =>
          i.toString
        case s: String =>
          if (n.getLiteralIsXML) {
            "<" + s
          } else {
            n.toString
          }
        case other@_ =>
          throw new UnsupportedOperationException(s"Literal $n not supported.")
      }
    } else if (n.isBlank) {
      s"_${n.getBlankNodeLabel}"
    } else {
      throw new UnsupportedOperationException(s"TripleRush does not support node $n of type ${n.getClass.getSimpleName}.")
    }
    // The code below can be enabled for debugging: it checks that a node is encoded such that decoding the string.
    // results in a node that is equal to the original node.
    //val thereAnBack = stringToNode(nodeAsString)
    //assert(thereAnBack.equals(n), s"Node $n as a string is $nodeAsString, which gets converted to $thereAnBack")
    nodeAsString
  }

  def stringToNode(s: String): Node = {
    s.head match {
      case c: Char if (c.isLetter) =>
        NodeFactory.createURI(s)
      case c: Char if (c == '-' || c.isDigit) =>
        val dt = TypeMapper.getInstance.getTypeByName("http://www.w3.org/2001/XMLSchema#integer")
        val label = LiteralLabelFactory.create(s, dt)
        NodeFactory.createLiteral(label)
      case '"' =>
        if (s.last == '"') {
          // Simple case, no type or language tag.
          NodeFactory.createLiteral(s.substring(1, s.length - 1))
        } else {
          val tagIndex = s.lastIndexOf('@')
          val tpeIndex = s.lastIndexOf('^')
          if (tpeIndex > tagIndex) {
            // Yes, we have a type.
            val typeName = s.substring(tpeIndex + 1)
            val dt = TypeMapper.getInstance.getTypeByName(typeName)
            val lex = s.substring(1, tpeIndex - 2)
            val label = LiteralLabelFactory.create(lex, dt)
            NodeFactory.createLiteral(label)
          } else if (tagIndex > 0) {
            val lex = s.substring(1, tagIndex - 1)
            val lang = s.substring(tagIndex + 1)
            val label = LiteralLabelFactory.create(lex, lang)
            NodeFactory.createLiteral(label)
          } else {
            throw new UnsupportedOperationException(s"Could not convert literal $s to a node.")
          }
        }
      case '<' =>
        NodeFactory.createLiteral(s.tail, null, true)
      case '_' =>
        NodeFactory.createBlankNode(BlankNodeId.create(s.tail))
      case other: Char =>
        throw new UnsupportedOperationException(s"Encoded string $s could not be decoded.")
    }
  }

}
