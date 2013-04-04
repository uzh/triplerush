package com.signalcollect.triplerush.evaluation

import com.signalcollect.triplerush._
import scala.collection.JavaConversions._
import com.hp.hpl.jena.query._
import com.hp.hpl.jena.sparql.syntax._

object PatternQueryParser {
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