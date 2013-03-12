package com.signalcollect.pathqueries

import scala.language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global

import SparqlDsl._

object PatternQueriesExample extends App {
  val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
  val query1Dsl = SELECT ? "x" WHERE (
    | - "x" - s"$ub#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
    | - "x" - s"$rdf#type" - s"$ub#GraduateStudent")

  val qe = new QueryEngine
  println("Loading triples ...")
  qe.load("./uni0-0.nt")
  println("Executing query ...")
  qe.executeQuery(query1Dsl) onSuccess {
    case results =>
      println("Result bindings:")
      results foreach { result =>
        println("\t" + result.bindings)
      }
  }
  qe.shutdown

  //  for (fileNumber <- 0 to 0) {
  //    val filename = s"./uni0-$fileNumber.nt"
  //    print(s"loding $filename ...")
  //    load(filename)
  //    println(" done")
  //  }
}








