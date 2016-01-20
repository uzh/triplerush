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

import java.io.InputStream
import org.apache.jena.riot.Lang
import com.signalcollect.triplerush.loading.TripleIterator
import scala.language.implicitConversions
import org.apache.jena.graph.{ Triple => JenaTriple }
import com.signalcollect.triplerush.dictionary.RdfDictionary
import com.signalcollect.triplerush.sparql.NodeConversion
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ConvenienceOperations {

  def toTriplePattern(triple: JenaTriple, dictionary: RdfDictionary, blankNodeNamespace: Option[BlankNodeNamespace]): TriplePattern = {
    val sId = NodeConversion.nodeToId(triple.getSubject, dictionary, blankNodeNamespace)
    val pId = NodeConversion.nodeToId(triple.getPredicate, dictionary, blankNodeNamespace)
    val oId = NodeConversion.nodeToId(triple.getObject, dictionary, blankNodeNamespace)
    TriplePattern(sId, pId, oId)
  }

}

trait ConvenienceOperations {
  this: TripleRush =>

  import ConvenienceOperations._

  def asyncLoadFromStream(inputStream: InputStream, blankNodeNamespace: Option[BlankNodeNamespace]): Future[Unit] = {
    val iterator = TripleIterator(inputStream)
    asyncAddTriples(iterator, blankNodeNamespace)
  }

  /**
   * String encoding:
   * By default something is interpreted as an IRI.
   * If something starts with a hyphen or a digit, it is interpreted as an integer literal
   * If something starts with '"' it is interpreted as a string literal.
   * If something has an extra '<' prefix, then the remainder is interpreted as an XML literal.
   * If something starts with '_', then the remainder is assumed to be a blank node ID where uniqueness is the
   * responsibility of the caller.
   */
  def asyncAddStringTriple(s: String, p: String, o: String): Future[Unit] = {
    val sId = dictionary(s)
    val pId = dictionary(p)
    val oId = dictionary(o)
    asyncAddEncodedTriple(sId, pId, oId)
  }

  /**
   * String encoding:
   * By default something is interpreted as an IRI.
   * If something starts with a hyphen or a digit, it is interpreted as an integer literal
   * If something starts with '"' it is interpreted as a string literal.
   * If something has an extra '<' prefix, then the remainder is interpreted as an XML literal.
   * If something starts with '_', then the remainder is assumed to be a blank node ID where uniqueness is the
   * responsibility of the caller.
   */
  def asyncAddStringTriples(i: Iterator[(String, String, String)]): Future[Unit] = {
    val mappedIterator = i.map {
      case (s, p, o) =>
        val sId = dictionary(s)
        val pId = dictionary(p)
        val oId = dictionary(o)
        TriplePattern(sId, pId, oId)
    }
    asyncAddTriplePatterns(mappedIterator)
  }

  def asyncAddTriple(triple: JenaTriple, blankNodeNamespace: Option[BlankNodeNamespace] = None): Future[Unit] = {
    asyncAddTriplePattern(toTriplePattern(triple, dictionary, blankNodeNamespace))
  }

  def asyncAddTriples(i: Iterator[JenaTriple], blankNodeNamespace: Option[BlankNodeNamespace] = None): Future[Unit] = {
    val mappedIterator = i.map(toTriplePattern(_, dictionary, blankNodeNamespace))
    asyncAddTriplePatterns(mappedIterator)
  }

  def asyncAddTriplePattern(tp: TriplePattern): Future[Unit] = {
    asyncAddEncodedTriple(tp.s, tp.p, tp.o)
  }

  // Delegates, just to implement the interface.
  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]] = {
    resultIteratorForQuery(query, None, Long.MaxValue)
  }

}
