/*
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

package com.signalcollect.triplerush

import java.io.InputStream
import org.apache.jena.riot.Lang
import com.signalcollect.triplerush.loading.TripleIterator
import scala.language.implicitConversions
import org.apache.jena.graph.{ Triple => JenaTriple }
import com.signalcollect.triplerush.loading.DataLoader

object Conversions {

  implicit def filePathToTripleIterator(filePath: String): TripleIterator = {
    TripleIterator(filePath)
  }

}

trait ConvenienceOperations  {
  this: TripleRush with BlockingOperations =>

  def loadFromStream(
    inputStream: InputStream,
    lang: Lang,
    placementHint: Option[Long] = Some(OperationIds.embedInLong(OperationIds.nextId))): Unit = {
    val iterator = TripleIterator(inputStream, lang)
    loadFromIterator(iterator, placementHint)
  }

  def asyncLoadFromStream(
    inputStream: InputStream,
    lang: Lang,
    placementHint: Option[Long] = Some(OperationIds.embedInLong(OperationIds.nextId))): Unit = {
    val iterator = TripleIterator(inputStream, lang)
    asyncLoadFromIterator(iterator, placementHint)
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
  def addStringTriple(s: String, p: String, o: String): Unit = {
    val sId = dictionary(s)
    val pId = dictionary(p)
    val oId = dictionary(o)
    addEncodedTriple(sId, pId, oId)
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
  def asyncAddStringTriple(s: String, p: String, o: String): Unit = {
    val sId = dictionary(s)
    val pId = dictionary(p)
    val oId = dictionary(o)
    asyncAddEncodedTriple(sId, pId, oId)
  }

  def addTriple(triple: JenaTriple): Unit = {
    addTriplePattern(DataLoader.toTriplePattern(triple, dictionary))
  }

  def asyncAddTriple(triple: JenaTriple): Unit = {
    asyncAddTriplePattern(DataLoader.toTriplePattern(triple, dictionary))
  }

  def addTriples(i: Iterator[JenaTriple]): Unit = {
    val mappedIterator = i.map(DataLoader.toTriplePattern(_, dictionary))
    addTriplePatterns(mappedIterator)
  }

  def asynccAddTriples(i: Iterator[JenaTriple]): Unit = {
    val mappedIterator = i.map(DataLoader.toTriplePattern(_, dictionary))
    asyncAddTriplePatterns(mappedIterator)
  }

  def addTriplePattern(tp: TriplePattern): Unit = {
    addEncodedTriple(tp.s, tp.p, tp.o)
  }

  def asyncAddTriplePattern(tp: TriplePattern): Unit = {
    asyncAddEncodedTriple(tp.s, tp.p, tp.o)
  }

  // Delegates, just to implement the interface.
  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]] = {
    resultIteratorForQuery(query, None, Long.MaxValue)
  }

  def addEncodedTriple(sId: Int, pId: Int, oId: Int): Unit = {
    addEncodedTriple(sId, pId, oId, TripleRush.defaultBlockingOperationTimeout)
  }
  
}
