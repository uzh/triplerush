package com.signalcollect.triplerush.sparql

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.datatypes.TypeMapper

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
          n.toString
        case other =>
          throw new UnsupportedOperationException(s"Literal $n not supported.")
      }
    } else if (n.isBlank) {
      s"_${n.getBlankNodeLabel}"
    } else {
      throw new UnsupportedOperationException(s"TripleRush does not support node $n of type ${n.getClass.getSimpleName}.")
    }
    val thereAnBack = stringToNode(nodeAsString)
    assert(thereAnBack.equals(n), s"Node $n as a string is $nodeAsString, which gets converted to $thereAnBack")
    nodeAsString
  }

  def stringToNode(s: String): Node = {
    s.head match {
      case c: Char if (c.isLetter) =>
        NodeFactory.createURI(s)
      case c: Char if (c == '-' || c.isDigit) =>
        NodeFactory.createLiteral(LiteralLabelFactory.createTypedLiteral(c.toInt))
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
        throw new UnsupportedOperationException(s"XML literal $s is not supported.")
      case '_' =>
        NodeFactory.createAnon(AnonId.create(s.tail))
      case other =>
        throw new UnsupportedOperationException(s"Encoded string $s could not be decoded.")
    }
  }

}
