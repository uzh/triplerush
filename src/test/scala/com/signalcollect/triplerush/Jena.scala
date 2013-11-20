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
import collection.JavaConversions._
import com.hp.hpl.jena.query.QuerySolution

class Jena extends QueryEngine {
  val model = ModelFactory.createDefaultModel
  def addEncodedTriple(s: Int, p: Int, o: Int) {
    val resource = model.createResource(intToString(s))
    val prop = model.createProperty(intToString(p))
    val obj = model.createResource(intToString(o))
    model.add(resource, prop, obj)
    println(s"Added: $resource $prop $obj")
  }
  def executeQuery(q: Array[Int]): Future[QueryResult] = {
    println("executing query on model:")
    println("Model:\n" + model.listStatements.mkString("\n"))
    future {
      val patterns = q.patterns
      val variables = patterns flatMap (p => Set(p.s, p.p, p.o))
      val variableNames = variables filter (_ < 0) map (intToString)
      val queryString = s"""SELECT ${variableNames.mkString(" ")}	
                            WHERE
                            { ${patterns.map(patternToString).mkString(" .\n")} }"""
      println("Query: " + queryString)
      val query = QueryFactory.create(queryString)
      println("parsed query: " + query)
      val qe = QueryExecutionFactory.create(query, model)
      val results = qe.execSelect
      println(s"Number of results: ${results.size}")
      val trResults = results.map(transformJenaResult)
      val bufferResults = trResults.map(UnrolledBuffer(_)).foldLeft(UnrolledBuffer.empty[Array[Int]])(_.concat(_))
      qe.close
      QueryResult(bufferResults, Array(), Array())
    }
  }

  def transformJenaResult(s: QuerySolution): Array[Int] = {
    println("Transforming solution: " + s)
    val x = s.get("X")
    val y = s.get("Y")
    val z = s.get("Z")
    println("xyz = " + x + y + z)
    Array()
  }

  def patternToString(tp: TriplePattern): String = s"${intToString(tp.s)} ${intToString(tp.p)} ${intToString(tp.o)} ."
  def intToString(v: Int): String = {
    v match {
      case i if i > 0 => s"<${i.toString}>"
      case -1 => "?x"
      case -2 => "?y"
      case -3 => "?z"
      case other => throw new Exception("Unsupported variable.")
    }
  }
  def awaitIdle {}
  def shutdown {}
}