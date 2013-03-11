package com.signalcollect.pathqueries

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
  object select {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(List(s))
  }
  case class DslVariableDeclaration(variables: List[String]) {
    def ?(variableName: String): DslVariableDeclaration = DslVariableDeclaration(variableName :: variables)
    def where(triplePatterns: DslTriplePattern*): DslQuery = {
      DslQuery(variables, triplePatterns.toList)
    }
  }
  case class DslQuery(variables: List[String], dslTriplePatterns: List[DslTriplePattern])
  implicit def dsl2Query(q: DslQuery): Query = {
    q.variables foreach (Mapping.registerVariable(_))
    Query(q.dslTriplePatterns map (_.toTriplePattern))
  }
}