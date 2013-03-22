package com.signalcollect.triplerush

import scala.concurrent.Await
import com.signalcollect.triplerush.SELECT

object Benchmark extends App {
  val qe = new QueryEngine
  for (fileNumber <- 0 to 14) {
    val filename = s"./lubm/university0_$fileNumber.nt"
    print(s"loading $filename ...")
    qe.load(filename)
    println(" done")
  }
  val queries: List[PatternQuery] = List(
    // Query 1
    SELECT ? "X" WHERE (
      | - "X" - s"$ub#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
      | - "X" - s"$rdf#type" - s"$ub#GraduateStudent"),
    // Query 2
    SELECT ? "X" ? "Y" ? "Z" WHERE (
      | - "X" - s"$rdf#type" - s"$ub#GraduateStudent",
      | - "X" - s"$ub#memberOf" - "Z",
      | - "Z" - s"$rdf#type" - s"$ub#Department",
      | - "Z" - s"$ub#subOrganizationOf" - "Y",
      | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
      | - "Y" - s"$rdf#type" - s"$ub#University"),
    // Query 3
    SELECT ? "X" WHERE (
      | - "X" - s"$ub#publicationAuthor" - "http://www.Department0.University0.edu/AssistantProfessor0",
      | - "X" - s"$rdf#type" - s"$ub#Publication"),
    // Query 4
    SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
      | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
      | - "X" - s"$rdf#type" - s"$ub#Professor",
      | - "X" - s"$ub#name" - "Y1",
      | - "X" - s"$ub#emailAddress" - "Y2",
      | - "X" - s"$ub#telephone" - "Y3"),
    //Query 5
    SELECT ? "X" WHERE (
      | - "X" - s"$ub#memberOf" - "http://www.Department0.University0.edu",
      | - "X" - s"$rdf#type" - s"$ub#Person"),
    //Query 6
    SELECT ? "X" WHERE (
      | - "X" - s"$rdf#type" - s"$ub#Student"),
    //Query 7
    SELECT ? "X" ? "Y" WHERE (
      | - "http://www.Department0.University0.edu/AssociateProfessor0" - s"$ub#teacherOf" - "Y",
      | - "Y" - s"$rdf#type" - s"$ub#Course",
      | - "X" - s"$ub#takesCourse" - "Y",
      | - "X" - s"$rdf#type" - s"$ub#Student"),
    //Query 8
    SELECT ? "X" ? "Y" ? "Z" WHERE (
      | - "Y" - s"$ub#subOrganizationOf" - "http://www.University0.edu",
      | - "Y" - s"$rdf#type" - s"$ub#Department",
      | - "X" - s"$ub#memberOf" - "Y",
      | - "X" - s"$rdf#type" - s"$ub#Student",
      | - "X" - s"$ub#emailAddress" - "Z"),
    //Query 9
    SELECT ? "X" ? "Y" ? "Z" WHERE (
      | - "Y" - s"$rdf#type" - s"$ub#Faculty",
      | - "Y" - s"$ub#teacherOf" - "Z",
      | - "Z" - s"$rdf#type" - s"$ub#Course",
      | - "X" - s"$ub#advisor" - "Y",
      | - "X" - s"$ub#takesCourse" - "Z",
      | - "X" - s"$rdf#type" - s"$ub#Student"),
    //Query 10
    SELECT ? "X" WHERE (
      | - "X" - s"$ub#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
      | - "X" - s"$rdf#type" - s"$ub#Student"),
    //Query 11
    SELECT ? "X" WHERE (
      | - "X" - s"$ub#subOrganizationOf" - "http://www.University0.edu",
      | - "X" - s"$rdf#type" - s"$ub#ResearchGroup"),
    //Query 12
    SELECT ? "X" ? "Y" WHERE (
      | - "Y" - s"$ub#subOrganizationOf" - "http://www.University0.edu",
      | - "Y" - s"$rdf#type" - s"$ub#Department",
      | - "X" - s"$ub#worksFor" - "Y",
      | - "X" - s"$rdf#type" - s"$ub#Chair"),
    //Query 13
    SELECT ? "X" WHERE (
      | - "http://www.University0.edu" - s"$ub#hasAlumnus" - "X",
      | - "X" - s"$rdf#type" - s"$ub#Person"),
    //Query 14
    SELECT ? "X" WHERE (
      | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent")) 
  val resultFuture = qe.executeQuery(q)
  val result = Await.result(resultFuture, new FiniteDuration(100, TimeUnit.SECONDS))

}