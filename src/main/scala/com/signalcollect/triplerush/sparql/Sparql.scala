/*
 *  @author Philip Stutz
 *  
 *  Copyright 2014 University of Zurich
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

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import com.signalcollect.triplerush.Dictionary
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.util.ResultBindingsHashSet
import scala.collection.mutable.PriorityQueue
import com.signalcollect.triplerush.optimizers.Optimizer

/**
 * Class for SPARQL Query executions.
 */
case class Sparql(
  tr: TripleRush,
  optimizer: Option[Optimizer] = None,
  encodedPatternUnions: List[Seq[TriplePattern]],
  selectVariableIds: Set[Int] = Set.empty[Int],
  variableNameToId: Map[String, Int] = Map.empty[String, Int],
  idToVariableName: IndexedSeq[String] = Vector(),
  isDistinct: Boolean = false,
  orderBy: Option[Int] = None,
  limit: Option[Int] = None) {

  private val d = tr.dictionary

  assert(orderBy.isDefined == limit.isDefined, "ORDER BY and LIMIT are only supprted when both are used together.")

  protected val numberOfSelectVariables = selectVariableIds.size

  def withOptimizer(o: Optimizer) = this.copy(optimizer = Some(o))

  protected def lookupVariableBinding(encodedResult: Array[Int])(variableName: String): String = {
    val id = variableNameToId(variableName)
    val index = VariableEncoding.variableIdToDecodingIndex(id)
    val encodedBinding = encodedResult(index)
    d.unsafeDecode(encodedBinding)
  }

  protected class DecodingIterator(encodedIterator: Iterator[Array[Int]]) extends Iterator[String => String] {
    def next: String => String = {
      val nextEncoded = encodedIterator.next
      lookupVariableBinding(nextEncoded)
    }
    def hasNext = encodedIterator.hasNext
  }

  def resultIterator: Iterator[String => String] = {
    if (orderBy == None && limit == None) {
      new DecodingIterator(encodedResults)
    } else if (orderBy.isDefined && limit.isDefined) {
      val orderByIndex = VariableEncoding.variableIdToDecodingIndex(orderBy.get)
      @inline def orderByStringForBinding(bindings: Array[Int]) = d.unsafeDecode(bindings(orderByIndex))
      val iterator = encodedResults
      val topK = limit.get
      implicit val ordering = Ordering.by((bindings: Array[Int]) => orderByStringForBinding(bindings))
      val topKQueue = new PriorityQueue[Array[Int]]()(ordering)
      while (iterator.hasNext) {
        val bindings = iterator.next
        if (topKQueue.size < topK) {
          topKQueue += bindings
        } else {
          if (ordering.compare(topKQueue.head, bindings) > 0) {
            topKQueue.dequeue
            topKQueue += bindings
          }
        }
      }
      val topKEncodedResults = topKQueue.toList.sorted
      new DecodingIterator(topKEncodedResults.toIterator)
    } else {
      throw new UnsupportedOperationException("ORDER BY and LIMIT are only supported when both are used.")
    }
  }

  /**
   * Still ignores ORDER BY and LIMIT, as they are best handled during decoding.
   */
  def encodedResults: Iterator[Array[Int]] = {
    val fullIterator = fullEncodedResultIterator
    if (isDistinct == true) {
      new DistinctIterator(fullIterator)
    } else {
      fullIterator
    }
  }

  // && orderBy == None && limit == None

  protected def fullEncodedResultIterator: Iterator[Array[Int]] = {
    val iterators = encodedPatternUnions.map {
      patterns =>
        tr.resultIteratorForQuery(patterns, optimizer, Some(numberOfSelectVariables))
    }
    iterators.reduce(_ ++ _)
  }

}

object Sparql {

  /**
   *  If the query might have results returns Some(Sparql), else returns None.
   */
  def apply(query: String)(implicit tr: TripleRush): Option[Sparql] = {
    val d = tr.dictionary
    val parsed: ParsedSparqlQuery = SparqlParser.parse(query)
    var containsEntryThatIsNotInDictionary = false
    val prefixes = parsed.prefixes
    val select = parsed.select
    val selectVariableNames = select.selectVariableNames
    val numberOfSelectVariables = selectVariableNames.size
    var selectVariableIds = Set.empty[Int]
    var nextVariableId = -1
    var variableNameToId = Map.empty[String, Int]
    var idToVariableName = Vector.empty[String]
    for (varName <- selectVariableNames) {
      val id = encodeVariable(varName)
      selectVariableIds += id
    }

    def encodeVariable(variableName: String): Int = {
      val idOption = variableNameToId.get(variableName)
      if (idOption.isDefined) {
        idOption.get
      } else {
        val id = nextVariableId
        nextVariableId -= 1
        variableNameToId += variableName -> id
        idToVariableName = idToVariableName :+ variableName
        id
      }
    }

    def dictionaryEncodePatterns(patterns: Seq[ParsedPattern]): Seq[TriplePattern] = {

      def expandPrefix(prefixedString: String): String = {
        val prefixEnd = prefixedString.indexOf(':')
        if (prefixEnd == -1) {
          throw new Exception(s"Iri $prefixedString should have a prefix, but none was found.")
        }
        val prefix = prefixedString.substring(0, prefixEnd)
        try {
          val remainder = prefixedString.substring(prefixEnd + 1)
          val expanded = prefixes(prefix)
          expanded + remainder
        } catch {
          case t: Throwable =>
            throw new Exception(s"""Prefix "$prefix" was not declared or $prefixedString did not properly define a prefix.""")
        }
      }

      def encodeVariableOrIri(s: VariableOrBound): Int = {
        s match {
          case Variable(name) =>
            encodeVariable(name)
          case StringLiteral(s) =>
            val decoded = d.unsafeGetEncoded(s)
            if (decoded > 0) {
              decoded
            } else {
              // Literal not in store, no results.
              containsEntryThatIsNotInDictionary = true
              Int.MaxValue
            }
          case Iri(url) =>
            val expandedUrl = if (url.startsWith("http")) {
              url
            } else {
              expandPrefix(url)
            }
            val decoded = d.unsafeGetEncoded(expandedUrl)
            if (decoded > 0) {
              decoded
            } else {
              // Url not in store, no results.
              containsEntryThatIsNotInDictionary = true
              Int.MaxValue
            }
        }
      }

      val encodedPatterns = patterns.map {
        case ParsedPattern(s, p, o) =>
          TriplePattern(encodeVariableOrIri(s), encodeVariableOrIri(p), encodeVariableOrIri(o))
      }

      encodedPatterns
    }

    // Needs to happen before 'containsEntryThatIsNotInDictionary' check, because it modifies that flag as a side effect.
    val encodedPatternUnions = select.patternUnions.map(dictionaryEncodePatterns)

    if (containsEntryThatIsNotInDictionary) {
      None
    } else {
      Some(
        Sparql(
          tr = tr,
          encodedPatternUnions = encodedPatternUnions,
          selectVariableIds = selectVariableIds,
          variableNameToId = variableNameToId,
          idToVariableName = idToVariableName,
          isDistinct = parsed.select.isDistinct,
          orderBy = select.orderBy.map(variableNameToId),
          limit = select.limit))
    }
  }
}
