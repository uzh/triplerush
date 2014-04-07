/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
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

import scala.collection.JavaConversions._
import language.implicitConversions
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock
import com.hp.hpl.jena.sparql.syntax.ElementVisitor
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery
import com.hp.hpl.jena.sparql.syntax.ElementMinus
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock
import com.hp.hpl.jena.sparql.syntax.ElementExists
import com.hp.hpl.jena.sparql.syntax.ElementAssign
import com.hp.hpl.jena.sparql.syntax.ElementDataset
import com.hp.hpl.jena.sparql.syntax.ElementBind
import com.hp.hpl.jena.sparql.syntax.ElementUnion
import com.hp.hpl.jena.sparql.syntax.ElementOptional
import com.hp.hpl.jena.sparql.syntax.ElementService
import com.hp.hpl.jena.sparql.syntax.ElementData
import com.hp.hpl.jena.sparql.syntax.ElementNotExists
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import scala.collection.parallel.mutable.ParArray

/**
 * Represents a SPARQL query.
 */
case class QuerySpecification(
  unmatched: Seq[TriplePattern],
  tickets: Long = Long.MaxValue,
  selectVarIds: Option[Set[Int]] = None,
  variableNameToId: Option[Map[String, Int]] = None,
  idToVariableName: Option[Map[Int, String]] = None,
  distinct: Boolean = false) {

  def resultIterator(implicit tr: TripleRush): Iterator[Map[String, String]] = {
    ???
  }
  
  def withUnmatchedPatterns(u: Seq[TriplePattern]): QuerySpecification = {
    copy(unmatched = u)
  }

  def counts(variableId: Int, encodedResults: Traversable[Array[Int]]): Map[Int, Int] = {
    var counts = Map.empty[Int, Int].withDefaultValue(0)
    for (encodedResult <- encodedResults) {
      val binding = encodedResult(math.abs(variableId) - 1)
      val countForBinding = counts(binding)
      counts += binding -> { countForBinding + 1 }
    }
    counts
  }

  def decodeResults(encodedResults: Traversable[Array[Int]]): Option[Traversable[Map[String, String]]] = {
    if (variableNameToId.isDefined && idToVariableName.isDefined && selectVarIds.isDefined) {
      val parEncodedResults: ParArray[Array[Int]] = encodedResults.toArray.par
      val select = selectVarIds.get
      val varToId = variableNameToId.get
      val idToVar = idToVariableName.get
      val variables = varToId.keys
      val decodedResultMaps = parEncodedResults.map { encodedResults =>
        val numberOfBindings = encodedResults.length
        val decodedResultMap = select.map { variableId =>
          idToVar(variableId) -> Dictionary(encodedResults(-variableId - 1))
        }.toMap
        decodedResultMap
      }
      Some(decodedResultMaps.seq)
    } else {
      None
    }
  }

}

object QuerySpecification {

  def fromSparql(q: String): Option[QuerySpecification] = {
    val visitor = new JenaQueryVisitor(q)
    visitor.createSpecification
  }

  class JenaQueryVisitor(queryString: String) extends ElementVisitor {
    private val jenaQuery = QueryFactory.create(queryString)
    private val selectVarNames = jenaQuery.getProjectVars.map(_.getVarName).toSet

    private var hasNoResults = false
    var nextVariableId = -1
    var variableNameToId = Map[String, Int]()
    var idToVariableName = Map[Int, String]()
    private var patterns: List[TriplePattern] = List()

    for (varName <- selectVarNames) {
      addVariableEncoding(varName)
    }

    def addVariableEncoding(variableName: String): Int = {
      val idOption = variableNameToId.get(variableName)
      if (idOption.isDefined) {
        idOption.get
      } else {
        val id = nextVariableId
        nextVariableId -= 1
        variableNameToId += variableName -> id
        idToVariableName += id -> variableName
        id
      }
    }

    private val queryPatterns = jenaQuery.getQueryPattern
    private var problem: Option[String] = {
      if (!jenaQuery.isStrict) {
        Some("Only strict queries are supported.")
      } else if (jenaQuery.isReduced) {
        Some("Feature REDUCED is unsupported.")
      } else if (jenaQuery.isOrdered) {
        Some("Feature ORDERED is unsupported.")
      } else if (jenaQuery.isQueryResultStar) {
        Some("Result variables as * is unsupported.")
      } else {
        None
      }
    }

    /**
     * Turns the sparql query string into a query specification,
     * returns None if at least one string that appears in the query has no dictionary encoding,
     * because currently such queries cannot have any results.
     */
    def createSpecification: Option[QuerySpecification] = {
      queryPatterns.visit(this)
      if (problem.isDefined) {
        throw new Exception(problem.get)
      } else {
        if (hasNoResults) {
          None
        } else {
          val selectVarIds = selectVarNames.map(variableNameToId)
          Some(QuerySpecification(
            unmatched = patterns,
            selectVarIds = Some(selectVarIds),
            variableNameToId = Some(variableNameToId),
            idToVariableName = Some(idToVariableName),
            distinct = jenaQuery.isDistinct))
        }
      }
    }

    def visit(el: ElementGroup) {
      for (element <- el.getElements) {
        println(element.getClass)
        element.visit(this)
      }
    }

    def visit(el: ElementPathBlock) {
      for (pattern <- el.patternElts) {
        val triple = pattern.asTriple
        val tripleComponents = Vector(triple.getSubject, triple.getPredicate, triple.getObject)
        val tripleIds = tripleComponents map { e =>
          if (e.isVariable) {
            val variableName = e.getName
            addVariableEncoding(variableName)
          } else if (e.isLiteral) {
            val literalString: String = e.getLiteral.toString
            if (Dictionary.contains(literalString)) {
              Dictionary(literalString)
            } else {
              //println(s"$literalString not in store, no results.")
              hasNoResults = true
              Int.MaxValue
            }
          } else {
            val uriString = e.getURI
            if (Dictionary.contains(uriString)) {
              Dictionary(uriString)
            } else {
              //println(s"$uriString not in store, no results.")
              hasNoResults = true
              Int.MaxValue
            }
          }
        }
        patterns = patterns ::: List(TriplePattern(tripleIds(0), tripleIds(1), tripleIds(2)))
      }
    }

    private def unsupported(el: Element) = {
      throw new UnsupportedOperationException(el.toString)
    }
    def visit(el: ElementTriplesBlock) = unsupported(el)
    def visit(el: ElementFilter) = unsupported(el)
    def visit(el: ElementAssign) = unsupported(el)
    def visit(el: ElementBind) = unsupported(el)
    def visit(el: ElementData) = unsupported(el)
    def visit(el: ElementUnion) = unsupported(el)
    def visit(el: ElementOptional) = unsupported(el)
    def visit(el: ElementDataset) = unsupported(el)
    def visit(el: ElementNamedGraph) = unsupported(el)
    def visit(el: ElementExists) = unsupported(el)
    def visit(el: ElementNotExists) = unsupported(el)
    def visit(el: ElementMinus) = unsupported(el)
    def visit(el: ElementService) = unsupported(el)
    def visit(el: ElementSubQuery) = unsupported(el)
  }
}