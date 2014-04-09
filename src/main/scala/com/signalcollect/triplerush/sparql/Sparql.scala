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
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush

/**
 * Class for SPARQL Query executions.
 */
case class Sparql(
  tr: TripleRush,
  encodedPatternUnions: List[Seq[TriplePattern]],
  selectVariableIds: List[Int],
  variableNameToId: Map[String, Int],
  idToVariableName: Array[String],
  isDistinct: Boolean = false,
  orderBy: Option[Int],
  limit: Option[Int]) {

  assert(orderBy.isDefined == limit.isDefined, "ORDER BY and LIMIT are only supprted when both are used together.")

  def encodedResults: Iterator[Array[Int]] = {
    // Simple case first.
    if (isDistinct == false && orderBy == None && limit == None) {
      val iterators = encodedPatternUnions.map {
        patterns =>
          val query = QuerySpecification(patterns)
          tr.resultIteratorForQuery(query)
      }
      iterators.reduce(_ ++ _)
    } else {
      throw new UnsupportedOperationException("Features DISTINCT, ORDER BY and LIMIT are not supported yet.")
    }

  }

  //  def counts(variableId: Int, encodedResults: Traversable[Array[Int]]): Map[Int, Int] = {
  //    var counts = Map.empty[Int, Int].withDefaultValue(0)
  //    for (encodedResult <- encodedResults) {
  //      val binding = encodedResult(math.abs(variableId) - 1)
  //      val countForBinding = counts(binding)
  //      counts += binding -> { countForBinding + 1 }
  //    }
  //    counts
  //  }
  //
  //  def decodeResults(encodedResults: Traversable[Array[Int]]): Option[Traversable[Map[String, String]]] = {
  //    if (variableNameToId.isDefined && idToVariableName.isDefined && selectVarIds.isDefined) {
  //      val parEncodedResults: ParArray[Array[Int]] = encodedResults.toArray.par
  //      val select = selectVarIds.get
  //      val varToId = variableNameToId.get
  //      val idToVar = idToVariableName.get
  //      val variables = varToId.keys
  //      val decodedResultMaps = parEncodedResults.map { encodedResults =>
  //        val numberOfBindings = encodedResults.length
  //        val decodedResultMap = select.map { variableId =>
  //          idToVar(variableId) -> Dictionary(encodedResults(-variableId - 1))
  //        }.toMap
  //        decodedResultMap
  //      }
  //      Some(decodedResultMaps.seq)
  //    } else {
  //      None
  //    }
  //  }

}

object Sparql {
  // Index -1 gets mapped to index 0, -2 to 1, etc.
  @inline private def idToIndex(id: Int) = math.abs(id) - 1

  /**
   *  If the query might have results returns Some(Sparql), else returns None.
   */
  def apply(query: String)(implicit tr: TripleRush): Option[Sparql] = {
    val parsed: ParsedSparqlQuery = SparqlParser.parse(query)
    var containsEntryThatIsNotInDictionary = false
    val prefixes = parsed.prefixes
    val select = parsed.select
    val selectVariableNames = select.selectVariableNames
    val numberOfSelectVariables = selectVariableNames.size
    var selectVariableIds = List.empty[Int]
    var nextVariableId = -1
    var variableNameToId = Map.empty[String, Int]
    var idToVariableName = new Array[String](numberOfSelectVariables)
    for (varName <- selectVariableNames) {
      val id = encodeVariable(varName)
      selectVariableIds = id :: selectVariableIds
    }

    def encodeVariable(variableName: String): Int = {
      val idOption = variableNameToId.get(variableName)
      if (idOption.isDefined) {
        idOption.get
      } else {
        val id = nextVariableId
        nextVariableId -= 1
        variableNameToId += variableName -> id
        idToVariableName(idToIndex(id)) = variableName
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
          case Variable(name) => encodeVariable(name)
          case Iri(url) =>
            val expandedUrl = if (url.startsWith("http")) {
              url
            } else {
              expandPrefix(url)
            }
            if (Dictionary.contains(expandedUrl)) {
              Dictionary(expandedUrl)
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
