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
import com.hp.hpl.jena.rdf.model.RDFNode

class Jena extends QueryEngine {
  val model = ModelFactory.createDefaultModel
  def addEncodedTriple(s: Int, p: Int, o: Int) {
    val resource = model.createResource(intToInsertString(s))
    val prop = model.createProperty(intToInsertString(p))
    val obj = model.createResource(intToInsertString(o))
    model.add(resource, prop, obj)
  }
  def executeQuery(q: Array[Int]): Future[QueryResult] = {
    future {
      val patterns = q.patterns
      val variableNames = {
        val vars = patterns.
          flatMap(p => Set(p.s, p.p, p.o)).
          filter(_ < 0).
          map(intToQueryString).
          distinct
        if (vars.isEmpty) {
          List("*")
        } else {
          vars
        }
      }
      val queryString = s"""
PREFIX ns: <http://example.com#>
SELECT ${variableNames.mkString(" ")}	
WHERE {
\t${patterns.map(patternToString).mkString(" \n\t")} }"""
      val query = QueryFactory.create(queryString)
      val qe = QueryExecutionFactory.create(query, model)
      val results = qe.execSelect.toList
      val trResults = results.map(transformJenaResult)
      val bufferResults = trResults.map(
        UnrolledBuffer(_)).foldLeft(
          UnrolledBuffer.empty[Array[Int]])(_.concat(_))
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
    Array(exampleToInt(x), exampleToInt(y), exampleToInt(z))
  }

  def exampleToInt(r: RDFNode): Int = {
    if (r == null) {
      Int.MinValue
    } else {
      val s = r.toString.substring(19)
      val decoded = s(0) - a
      decoded
    }
  }

  val a: Char = 'a'
  def patternToString(tp: TriplePattern): String = s"${intToQueryString(tp.s)} ${intToQueryString(tp.p)} ${intToQueryString(tp.o)} ."
  def intToQueryString(v: Int): String = {
    v match {
      // map to characters
      //case i if i > 0 => s"<${i.toString}>"
      case i if i > 0 => "ns:" + (a + i).toChar
      case -1 => "?X"
      case -2 => "?Y"
      case -3 => "?Z"
      case other => throw new Exception("Unsupported variable.")
    }
  }
  def intToInsertString(v: Int): String = {
    if (v > 0) {
      "http://example.com#" + (a + v).toChar
    } else {
      throw new Exception(s"Unsupported value $v.")
    }
  }
  def awaitIdle {}
  def shutdown {}
}
