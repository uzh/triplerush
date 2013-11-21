//package com.signalcollect.triplerush
//
//import scala.concurrent.Future
//import scala.concurrent.future
//import com.signalcollect.triplerush.vertices.QueryResult
//import scala.collection.mutable.UnrolledBuffer
//import scala.concurrent.ExecutionContext.Implicits.global
//import com.signalcollect.triplerush.QueryParticle._
//import collection.JavaConversions._
//import org.openrdf.repository.sail.SailRepository
//import org.openrdf.sail.memory.MemoryStore
//import java.io.StringWriter
//import org.openrdf.rio.Rio
//import java.util.ArrayList
//import java.util.HashMap
//
//class Sesame extends QueryEngine {
//  val repo = new SailRepository(new MemoryStore())
//  repo.initialize
//
//  def addEncodedTriple(s: Int, p: Int, o: Int) {
//    val con = repo.getConnection
//    try {
//      val f = con.getValueFactory
//      val sUri = f.createURI(intToInsertString(s))
//      val pUri = f.createURI(intToInsertString(p))
//      val oUri = f.createURI(intToInsertString(o))
//      val st = f.createStatement(sUri, pUri, oUri)
//      con.add(st)
//      println(s"Added: $sUri $pUri $oUri")
//    } finally {
//      con.close
//    }
//  }
//  def executeQuery(q: Array[Int]): Future[QueryResult] = {
//    println("Executing query on model:")
//    val con = repo.getConnection
//    future {
//      try {
//        val f = con.getValueFactory
//        val patterns = q.patterns
//        val variables = patterns flatMap (p => Set(p.s, p.p, p.o))
//        val variableNames = variables filter (_ < 0) map (intToQueryString)
//        val queryString = s"""
//    PREFIX ns: <http://example.com#>
//    SELECT ${variableNames.mkString(" ")}	
//    WHERE {
//    \t${patterns.map(patternToString).mkString(" .\n\t")} }"""
//        println("Query: " + queryString)
//        val query = con.prepareTupleQuery(
//          org.openrdf.query.QueryLanguage.SPARQL, queryString)
//        println("Parsed query: " + query)
//        val qres = query.evaluate
//        val reslist = new ArrayList[HashMap[String, String]]
//        println(s"# results = ${reslist.length}")
//        while (qres.hasNext) {
//          val b = qres.next
//          val names = b.getBindingNames
//          val hm = new HashMap[String, String]
//          for (n <- names) {
//            val k = n
//            val v = b.getValue(n).toString
//            println(s"k=$k v=$v")
//            hm.put(k, v)
//          }
//          reslist.add(hm)
//        }
//        //reslist
//        null.asInstanceOf[QueryResult]
//      } catch {
//        case t: Throwable =>
//          println(t.getMessage)
//          t.printStackTrace
//          throw t
//      } finally {
//        con.close
//      }
//    }
//
//    //    println("Model:\n" + model.listStatements.mkString("\n"))
//    //    future {
//    //      val patterns = q.patterns
//    //      val variables = patterns flatMap (p => Set(p.s, p.p, p.o))
//    //      val variableNames = variables filter (_ < 0) map (intToQueryString)
//    //      val queryString = s"""
//    //PREFIX ns: <http://example.com#>
//    //SELECT ${variableNames.mkString(" ")}	
//    //WHERE {
//    //\t${patterns.map(patternToString).mkString(" .\n\t")} }"""
//    //      println("Query: " + queryString)
//    //      val query = QueryFactory.create(queryString)
//    //      println("parsed query: \n" + query)
//    //      val qe = QueryExecutionFactory.create(query, model)
//    //      val results = qe.execSelect
//    //      while (results.hasNext) {
//    //        println("WAAAH: " + results.next)
//    //      }
//    //      println(s"Number of results: ${results.size}")
//    //      val trResults = results.map(transformJenaResult)
//    //      val bufferResults = trResults.map(
//    //        UnrolledBuffer(_)).foldLeft(
//    //          UnrolledBuffer.empty[Array[Int]])(_.concat(_))
//    //      qe.close
//    //      QueryResult(bufferResults, Array(), Array())
//    //    }
//  }
//
//  //  def transformJenaResult(s: QuerySolution): Array[Int] = {
//  //    println("Transforming solution: " + s)
//  //    val x = s.get("X")
//  //    val y = s.get("Y")
//  //    val z = s.get("Z")
//  //    println("xyz = " + x + y + z)
//  //    Array()
//  //  }
//
//  val a: Char = 'a'
//  def patternToString(tp: TriplePattern): String = s"${intToQueryString(tp.s)} ${intToQueryString(tp.p)} ${intToQueryString(tp.o)} ."
//  def intToQueryString(v: Int): String = {
//    v match {
//      // map to characters
//      //case i if i > 0 => s"<${i.toString}>"
//      case i if i > 0 => "ns:" + (a + i).toChar
//      case -1 => "?x"
//      case -2 => "?y"
//      case -3 => "?z"
//      case other => throw new Exception("Unsupported variable.")
//    }
//  }
//  def intToInsertString(v: Int): String = {
//    v match {
//      // map to characters
//      case i if i > 0 => "<http://example.com#" + (a + i).toChar + ">"
//      case -1 => "?x"
//      case -2 => "?y"
//      case -3 => "?z"
//      case other => throw new Exception("Unsupported variable.")
//    }
//  }
//  def awaitIdle {}
//  def shutdown {}
//}
