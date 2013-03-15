package com.signalcollect.triplerush

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
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(List(s))
  }
  case class DslVariableDeclaration(variables: List[String]) {
    def ?(variableName: String): DslVariableDeclaration = DslVariableDeclaration(variableName :: variables)
    def WHERE(triplePatterns: DslTriplePattern*): DslQuery = {
      DslQuery(variables, triplePatterns.toList)
    }
  }
  case class DslQuery(variables: List[String], dslTriplePatterns: List[DslTriplePattern])
  implicit def dsl2Query(q: DslQuery): PatternQuery = {
    q.variables foreach (Mapping.register(_, isVariable = true))
    PatternQuery(0, q.dslTriplePatterns map (_.toTriplePattern))
  }
}