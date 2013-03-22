package com.signalcollect.triplerush

import com.signalcollect.triplerush.Expression.int2Expression

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
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(Long.MaxValue, List(s))
  }
  case class SAMPLE(samples: Long) {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(samples, List(s))
  }
  case class DslVariableDeclaration(samples: Long, variables: List[String]) {
    def ?(variableName: String): DslVariableDeclaration = DslVariableDeclaration(samples, variableName :: variables)
    def WHERE(triplePatterns: DslTriplePattern*): DslQuery = {
      DslQuery(samples, variables, triplePatterns.toList)
    }
  }
  case class DslQuery(samples: Long, variables: List[String], dslTriplePatterns: List[DslTriplePattern])
  implicit def dsl2Query(q: DslQuery): PatternQuery = {
    q.variables foreach (Mapping.register(_, isVariable = true))
    PatternQuery(0, q.dslTriplePatterns map (_.toTriplePattern), tickets = q.samples, isSamplingQuery = (q.samples < Long.MaxValue) )
  }
}