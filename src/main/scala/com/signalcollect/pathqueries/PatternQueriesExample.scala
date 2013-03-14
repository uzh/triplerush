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
  val query2Dsl = SELECT ? "x" ? "y" ? "z" WHERE (
    | - "x" - s"$rdf#type" - s"$ub#GraduateStudent",
    | - "x" - s"$ub#memberOf" - "z",
    | - "z" - s"$rdf#type" - s"$ub#Department",
    | - "z" - s"$ub#subOrganizationOf" - "y",
    | - "x" - s"$ub#undergraduateDegreeFrom" - "y",
    | - "y" - s"$rdf#type" - s"$ub#University") 
  val query3Dsl = SELECT ? "x" WHERE (
    | - "x" - s"$ub#publicationAuthor" - "http://www.Department0.University0.edu/AssistantProfessor0",
    | - "x" - s"$rdf#type" - s"$ub#Publication")

  val qe = new QueryEngine
  println("Loading triples ...")

  for (fileNumber <- 0 to 14) {
    val filename = s"./uni0-$fileNumber.nt"
    print(s"loding $filename ...")
    qe.load(filename)
    println(" done")
  }

  println("Executing query ...")
  qe.executeQuery(query3Dsl) onSuccess {
    case results =>
      println("Result bindings:")
      results foreach { result =>
        println("\t" + result.bindings)
      }
  }
  qe.shutdown
}








