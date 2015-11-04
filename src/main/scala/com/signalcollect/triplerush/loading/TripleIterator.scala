/*
 *  @author Marko Kolar
 *  @author Philip Stutz
 *
 *  Copyright 2015 Cotiviti
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

package com.signalcollect.triplerush.loading

import java.io.{ File, FileInputStream, InputStream }
import java.net.URI
import scala.collection.JavaConversions.asScalaIterator
import org.semanticweb.yars.nx
import org.semanticweb.yars.nx.parser.NxParser
import org.semanticweb.yars.turtle.TurtleParser
import org.apache.jena.{graph => jena}
import java.nio.charset.StandardCharsets
import org.apache.jena.riot.RDFDataMgr

object TripleIterator {

  def apply(is: InputStream, parser: TripleParser = NTriples): TripleIterator = {
    new TripleIterator(is, parser)
  }

  def apply(filePath: String): TripleIterator = {
    val inputStream = new FileInputStream(filePath)
    if (filePath.endsWith("nt") || filePath.endsWith("ntriples")) {
      new TripleIterator(inputStream, NTriples)
    } else {
      throw new UnsupportedOperationException(s"No parser found for file $filePath.")
    }
  }

}

/**
 * Unifying the different parser interfaces.
 */
trait TripleParser {
  def parse(is: InputStream): Iterator[Array[nx.Node]]
}

object NTriples extends TripleParser {
  def parse(is: InputStream) = {
    val parser = new NxParser
    parser.parse(is)
  }
}

class TripleIterator(
    inputStream: InputStream,
    parser: TripleParser = NTriples) extends Iterator[jena.Triple] with AutoCloseable {

  private[this] def jenaNode(nxNode: nx.Node): jena.Node = nxNode match {
    case bNode: nx.BNode       => jena.NodeFactory.createBlankNode(bNode.getLabel)
    case literal: nx.Literal   => jena.NodeFactory.createLiteral(literal.getLabel, literal.getLanguageTag)
    case resource: nx.Resource => jena.NodeFactory.createURI(resource.getLabel)
  }

  private[this] val tripleIterator = {
    val iterator = {
      parser.parse(inputStream).map { triple =>
        require(triple.size == 3)
        new jena.Triple(jenaNode(triple(0)), jenaNode(triple(1)), jenaNode(triple(2)))
      }
    }
    new AutoClosingIterator[jena.Triple](iterator) {
      def close(): Unit = inputStream.close
    }
  }

  def close(): Unit = inputStream.close()

  def next(): jena.Triple = tripleIterator.next

  def hasNext(): Boolean = tripleIterator.hasNext

  private[this] abstract class AutoClosingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
    private[this] var isClosed = false

    protected def close(): Unit

    override def hasNext: Boolean = {
      if (!isClosed && !iterator.hasNext) {
        close()
        isClosed = true
      }
      iterator.hasNext
    }
    override def next(): T = iterator.next()
  }

}
