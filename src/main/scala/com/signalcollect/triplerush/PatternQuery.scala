package com.signalcollect.triplerush

import scala.collection.JavaConversions._
import scala.language.dynamics
import scala.reflect.classTag
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
import com.hp.hpl.jena.sparql.syntax.ElementFetch
import com.hp.hpl.jena.sparql.syntax.ElementDataset
import com.hp.hpl.jena.sparql.syntax.ElementBind
import com.hp.hpl.jena.sparql.syntax.ElementUnion
import com.hp.hpl.jena.sparql.syntax.ElementOptional
import com.hp.hpl.jena.sparql.syntax.ElementService
import com.hp.hpl.jena.sparql.syntax.ElementData
import com.hp.hpl.jena.sparql.syntax.ElementNotExists
import com.hp.hpl.jena.sparql.syntax.ElementFilter

case class PatternQuery(
  queryId: Int,
  unmatched: List[TriplePattern],
  matched: List[TriplePattern] = List(),
  bindings: Bindings = Bindings(),
  fraction: Double = 1,
  explorationFactor: Double = 1,
  isFailed: Boolean = false) {
  
  override def toString = {
    matched.mkString("\n") + unmatched.mkString("\n") + bindings.toString
  }
  
  def nextTargetId: Option[TriplePattern] = {
    unmatched match {
      case next :: _ =>
        Some(next.routingAddress)
      case other =>
        None
    }
  }
  def bind(tp: TriplePattern): Option[PatternQuery] = {
    unmatched match {
      case unmatchedHead :: unmatchedTail =>
        val newBindings = unmatchedHead.bindingsFor(tp)
        if (newBindings.isDefined && bindings.isCompatible(newBindings.get)) {
          val bound = unmatchedHead.applyBindings(newBindings.get)
          Some(PatternQuery(
            queryId,
            unmatchedTail map (_.applyBindings(newBindings.get)),
            bound :: matched,
            bindings.merge(newBindings.get),
            fraction))
        } else {
          None
        }
      case other =>
        None
    }
  }
  def withId(newId: Int) = {
    PatternQuery(newId, unmatched, matched, bindings, fraction, explorationFactor, isFailed)
  }
  def split(splitFactor: Double) = {
    PatternQuery(queryId, unmatched, matched, bindings, fraction / splitFactor, explorationFactor, isFailed)
  }
  def failed = {
    PatternQuery(queryId, unmatched, matched, bindings, fraction, isFailed = true)
  }
}

object PatternQuery {
  def build(q: String): Either[PatternQuery, String] = {
    val visitor = new JenaQueryVisitor(q)
    visitor.getPatternQuery
  }

  class JenaQueryVisitor(queryString: String) extends ElementVisitor {
    private val jenaQuery = QueryFactory.create(queryString)
    private val queryPatterns = jenaQuery.getQueryPattern
    private var variables: Set[String] = Set()
    private var patterns: List[TriplePattern] = List()
    private var problem: Option[String] = if (!jenaQuery.isStrict) {
      Some("Only strict queries are supported.")
    } else if (jenaQuery.isDistinct) {
      Some("Feature DISTINCT is unsupported.")
    } else if (jenaQuery.isReduced) {
      Some("Feature REDUCED is unsupported.")
    } else if (jenaQuery.isOrdered) {
      Some("Feature ORDERED is unsupported.")
    } else if (jenaQuery.isQueryResultStar) {
      Some("Result variables as * is unsupported.")
    } else {
      None
    }
    def getPatternQuery: Either[PatternQuery, String] = {
      queryPatterns.visit(this)
      if (problem.isDefined) {
        Right(problem.get)
      } else {
        Left(PatternQuery(0, patterns))
      }
    }
    def visit(el: ElementGroup) {
      for (element <- el.getElements) {
        element.visit(this)
      }
    }
    def visit(el: ElementPathBlock) {
      for (pattern <- el.patternElts) {
        val triple = pattern.asTriple
        val tripleList = List(triple.getSubject, triple.getPredicate, triple.getObject)
        val idList = tripleList map { e =>
          if (e.isVariable) {
            Mapping.register(e.getName, isVariable = true)
          } else if (e.isLiteral) {
            Mapping.register(e.getLiteral.toString)
          } else {
            Mapping.register(e.getURI)
          }
        }
        patterns = patterns ::: List(TriplePattern(idList(0), idList(1), idList(2)))
      }
    }
    private def unsupported(el: Element) = throw new UnsupportedOperationException(el.toString)
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
    def visit(el: ElementFetch) = unsupported(el)
    def visit(el: ElementSubQuery) = unsupported(el)
  }
}