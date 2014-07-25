package com.signalcollect.triplerush.jena
import com.hp.hpl.jena.rdf.model.ModelFactory
import scala.collection.mutable.UnrolledBuffer
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.signalcollect.triplerush.QueryParticle._
import collection.JavaConversions._
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.rdf.model.RDFNode
import com.signalcollect.triplerush.QueryEngine
import com.signalcollect.triplerush.TriplePattern
import scala.Option.option2Iterable
import com.hp.hpl.jena.sparql.engine.ResultSetStream
import com.hp.hpl.jena.sparql.core.ResultBinding
import com.hp.hpl.jena.sparql.engine.binding.BindingProject
import com.hp.hpl.jena.sparql.engine.binding.Binding
import com.hp.hpl.jena.sparql.engine.binding.BindingProjectBase
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap

class Jena extends QueryEngine {
  val model = ModelFactory.createDefaultModel
  def addEncodedTriple(s: Int, p: Int, o: Int) {
    val resource = model.createResource(intToInsertString(s))
    val prop = model.createProperty(intToInsertString(p))
    val obj = model.createResource(intToInsertString(o))
    model.add(resource, prop, obj)
  }

  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]] = {
    executeQuery(query).iterator
  }

  def executeQuery(q: Seq[TriplePattern]): Iterable[Array[Int]] = {
    val variableNames = {
      val vars = q.
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
\t${q.map(patternToString).mkString(" \n\t")} }"""
    val query = QueryFactory.create(queryString)
    val qe = QueryExecutionFactory.create(query, model)
    val results = qe.execSelect
    val transformedResults = results.flatMap(transformJenaResult)
    val bufferResults = transformedResults.map(
      UnrolledBuffer(_)).foldLeft(
        UnrolledBuffer.empty[Array[Int]])(_.concat(_))
    qe.close
    bufferResults
  }

  def transformJenaResult(s: QuerySolution): Option[Array[Int]] = {
    val x = s.get("X")
    val y = s.get("Y")
    val z = s.get("Z")
    val a = s.get("A")
    val b = s.get("B")
    val c = s.get("C")
    if (x != null || y != null || z != null || a != null || b != null || c != null) {
      Some(Array(exampleToInt(x), exampleToInt(y), exampleToInt(z), exampleToInt(a), exampleToInt(b), exampleToInt(c)))
    } else {
      None
    }
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
      case i if i > 0 && i < 26 =>
        "ns:" + (a + i).toChar
      case -1 => "?X"
      case -2 => "?Y"
      case -3 => "?Z"
      case -4 => "?A"
      case -5 => "?B"
      case -6 => "?C"
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
  def shutdown {
    model.close
  }
  def prepareExecution {}
}
