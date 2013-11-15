package com.signalcollect.triplerush

import scala.concurrent.Future
import scala.concurrent.future
import com.signalcollect.triplerush.vertices.QueryResult
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.Model
import scala.collection.mutable.UnrolledBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.signalcollect.triplerush.QueryParticle._

class Jena extends QueryEngine {
  val model = ModelFactory.createDefaultModel
  def addEncodedTriple(s: Int, p: Int, o: Int) {
    val resource = model.createResource(s.toString)
    val prop = model.createProperty(p.toString)
    val obj = model.createResource(o.toString)
    model.add(resource, prop, obj)
  }
  def executeQuery(q: Array[Int]): Future[QueryResult] = {
    future {
      val patterns = q.patterns
      val variables = patterns flatMap (p => Set(p.s, p.p, p.o))
      val variableNames = variables map (intToString)
      //      for (pattern <- q.patterns) {
      //      }
      val queryString = s"""SELECT ${variableNames.mkString(", ")}	
WHERE
{?X rdf:type ub:GraduateStudent .
  ?X ub:takesCourse "http://www.Department0.University0.edu/GraduateCourse0"}"""

      val query = QueryFactory.create(queryString)
      val qe = QueryExecutionFactory.create(query, model)
      val results = qe.execSelect
      //ResultSetFormatter.out(System.out, results, query);
      qe.close
      QueryResult(UnrolledBuffer(), Array(), Array())
    }
  }
  def patternToString(tp: TriplePattern): String = s"{ .\n ${intToString(tp.s)} ${intToString(tp.p)} ${intToString(tp.o)}}"
  def intToString(v: Int): String = {
    v match {
      case i if i > 0 => i.toString
      case -1 => "?X"
      case -2 => "?Y"
      case -3 => "?Z"
      case other => throw new Exception("Unsupported variable.")
    }
  }
  def awaitIdle {}
  def shutdown {}
}