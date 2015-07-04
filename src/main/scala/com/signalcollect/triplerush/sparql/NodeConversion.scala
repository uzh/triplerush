package com.signalcollect.triplerush.sparql

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.rdf.model.AnonId

object NodeConversion {

  def nodeToString(n: Node): String = {
    if (n.isURI) {
      n.toString(null, false)
    } else if (n.isLiteral) {
      n.getLiteralValue match {
        case i: Integer =>
          println(s"Integer literal $i converted to ${i.toString}")
          i.toString
        case s: String =>
          val l = n.toString
          println(s"General literal $n converted to ${l.toString}")
          l
        case other =>
          throw new UnsupportedOperationException(s"Literal $n not supported.")
      }
    } else if (n.isBlank) {
      val b = s"_${n.getBlankNodeLabel}"
      println(s"Blank node $n converted to $b")
      b
    } else {
      throw new UnsupportedOperationException(s"TripleRush does not support node $n of type ${n.getClass.getSimpleName}.")
    }
  }

  def stringToNode(s: String): Node = {
    s.charAt(0) match {
      case c: Char if (c.isLetter) =>
        val n = NodeFactory.createURI(s)
        println(s"Detected IRI $s, decoded as ${n.toString}")
        n
      case c: Char if (c == '-' || c.isDigit) =>
        val i = NodeFactory.createLiteral(LiteralLabelFactory.createTypedLiteral(c.toInt))
        println(s"Detected encoded integer literal $s, decoded as ${i.toString}")
        i
      case c: Char if (c == '"' || c == '<') =>
        val l = NodeFactory.createLiteral(s)
        println(s"Jena created literal $l from string $s")
        l
      case '_' =>
        val l = NodeFactory.createAnon(AnonId.create(s.tail))
        println(s"Jena created blank node $l from string $s")
        l
      case other =>
        throw new UnsupportedOperationException(s"Encoded string $s could not be decoded.")
    }
  }

}
