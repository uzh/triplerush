package com.signalcollect.triplerush.evaluation

import com.signalcollect.triplerush._
import scala.language.implicitConversions

object SparqlDsl extends App {
  object | {
    def -(s: String): DslS = DslS(s)
  }
  case class DslS(s: String) {
    def -(p: String) = DslSp(s, p)
  }
  case class DslSp(s: String, p: String) {
    def -(o: String) = DslTriplePattern(s, p, o)
  }
  case class DslTriplePattern(s: String, p: String, o: String) {
    def toTriplePattern: TriplePattern = {
      TriplePattern(Mapping.register(s), Mapping.register(p), Mapping.register(o))
    }
  }
  object SELECT {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = false, Long.MaxValue, List(s))
  }
  case class SAMPLE(tickets: Long) {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = true, tickets, List(s))
  }
  case class BOUNDED(tickets: Long) {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = false, tickets, List(s))
  }
  case class DslVariableDeclaration(isSamplingQuery: Boolean, tickets: Long, variables: List[String]) {
    def ?(variableName: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery, tickets, variableName :: variables)
    def WHERE(triplePatterns: DslTriplePattern*): DslQuery = {
      DslQuery(isSamplingQuery, tickets, variables, triplePatterns.toList)
    }
  }
  case class DslQuery(isSamplingQuery: Boolean, tickets: Long, variables: List[String], dslTriplePatterns: List[DslTriplePattern])
  implicit def dsl2Query(q: DslQuery): PatternQuery = {
    q.variables foreach (Mapping.register(_, isVariable = true))
    val queryId =  if (q.isSamplingQuery) QueryIds.nextSamplingQueryId  else QueryIds.nextFullQueryId
    PatternQuery(queryId, q.dslTriplePatterns map (_.toTriplePattern), tickets = q.tickets)
  }
}