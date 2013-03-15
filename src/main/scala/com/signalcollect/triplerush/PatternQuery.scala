package com.signalcollect.triplerush

import com.hp.hpl.jena.sparql.syntax.ElementGroup
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock
import com.hp.hpl.jena.sparql.syntax.ElementVisitor
import collection.JavaConversions._
import language.dynamics
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.syntax.Element

object PatternQuery {
  object JenaQueryVisitor extends Dynamic with ElementVisitor {
    var result: Either[PatternQuery, String] = Right("No query processed yet.")
    def applyDynamic(methodName: String)(args: Any*): Any = {
      if (methodName == "visit" && args.length == 1) {
        args(0) match {
          case e: ElementPathBlock =>
            for (pattern <- e.patternElts) {
              println("this is a pattern: " + pattern)
              val triple = pattern.asTriple
              val s = triple.getSubject
              if (s.isVariable()) {
                println(s.getName)
              } else {
                println(s.getLiteral)
              }
              val p = triple.getPredicate
              println(p.toString)
              val o = triple.getObject
              println(o.toString)
            }
          case e: ElementGroup =>
            for (element <- e.getElements) {
              element.visit(this)
            }
          case other =>
        }
      } else if (methodName == "getQuery") {
        result
      }
    }
  }

  def build(q: String): Either[PatternQuery, String] = {
    //  if (query.isSelectType) {
    //val myVistor = 
    //isStrict
    //isDistinct
    //isReduced
    //isOrdered
    //isQueryResultStar
    //val elements = query.getQueryPattern
    //elements.visit(myVistor)
    //elements.println()
    val jenaQuery = QueryFactory.create(q).getQueryPattern
    JenaQueryVisitor.getQuery(jenaQuery).asInstanceOf[Either[PatternQuery, String]]
  }

}

case class PatternQuery(
  queryId: Int,
  unmatched: List[TriplePattern],
  matched: List[TriplePattern] = List(),
  bindings: Bindings = Bindings(),
  fraction: Double = 1,
  isFailed: Boolean = false) {
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
    PatternQuery(newId, unmatched, matched, bindings, fraction, isFailed)
  }
  def split(splitFactor: Double) = {
    PatternQuery(queryId, unmatched, matched, bindings, fraction / splitFactor, isFailed)
  }
  def failed = {
    PatternQuery(queryId, unmatched, matched, bindings, fraction, isFailed = true)
  }
}