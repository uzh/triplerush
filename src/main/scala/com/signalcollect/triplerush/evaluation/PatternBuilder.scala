/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
 *      
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */

package com.signalcollect.triplerush.evaluation

import java.util.HashMap
import collection.JavaConversions._
import scala.io.Source
import com.signalcollect.triplerush.TriplePattern
import scala.io.Codec

//    def fullQueries: List[PatternQuery] = List(
//      SELECT ? "X" ? "Y" ? "Z" WHERE (
//        | - "X" - s"$rdf#type" - s"$ub#GraduateStudent",
//        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
//        | - "X" - s"$ub#memberOf" - "Z",
//        | - "Z" - s"$rdf#type" - s"$ub#Department",
//        | - "Z" - s"$ub#subOrganizationOf" - "Y",
//        | - "Y" - s"$rdf#type" - s"$ub#University"),
//      SELECT ? "X" ? "Y" WHERE (
//        | - "X" - s"$rdf#type" - s"$ub#Course",
//        | - "X" - s"$ub#name" - "Y"),
//      SELECT ? "X" ? "Y" ? "Z" WHERE (
//        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
//        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent",
//        | - "X" - s"$ub#memberOf" - "Z",
//        | - "Z" - s"$ub#subOrganizationOf" - "Y",
//        | - "Z" - s"$rdf#type" - s"$ub#Department",
//        | - "Y" - s"$rdf#type" - s"$ub#University"),
//      SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
//        | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
//        | - "X" - s"$rdf#type" - s"$ub#FullProfessor",
//        | - "X" - s"$ub#name" - "Y1",
//        | - "X" - s"$ub#emailAddress" - "Y2",
//        | - "X" - s"$ub#telephone" - "Y3"),
//      SELECT ? "X" WHERE (
//        | - "X" - s"$ub#subOrganizationOf" - "http://www.Department0.University0.edu",
//        | - "X" - s"$rdf#type" - s"$ub#ResearchGroup"),
//      SELECT ? "X" ? "Y" WHERE (
//        | - "Y" - s"$ub#subOrganizationOf" - "http://www.University0.edu",
//        | - "Y" - s"$rdf#type" - s"$ub#Department",
//        | - "X" - s"$ub#worksFor" - "Y",
//        | - "X" - s"$rdf#type" - s"$ub#FullProfessor"),
//      SELECT ? "X" ? "Y" ? "Z" WHERE (
//        | - "Y" - s"$rdf#type" - s"$ub#FullProfessor",
//        | - "Y" - s"$ub#teacherOf" - "Z",
//        | - "Z" - s"$rdf#type" - s"$ub#Course",
//        | - "X" - s"$ub#advisor" - "Y",
//        | - "X" - s"$ub#takesCourse" - "Z",
//        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent"))

case class TextTriplePattern(s: String, p: String, o: String)

object LubmQueryBuilder extends App {
  val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"

  val builder = new PatternBuilder("./lubm160/dictionary.txt")
  val lubmQuery1Patterns = builder.build(
    List(
      TextTriplePattern("?X", s"$rdf#type", s"$ub#GraduateStudent"),
      TextTriplePattern("?X", s"$ub#undergraduateDegreeFrom", "?Y"),
      TextTriplePattern("?X", s"$ub#memberOf", "?Z"),
      TextTriplePattern("?Z", s"$rdf#type", s"$ub#Department"),
      TextTriplePattern("?Z", s"$ub#subOrganizationOf", "?Y"),
      TextTriplePattern("?Y", s"$rdf#type", s"$ub#University")))
  val lubmQuery2Patterns = builder.build(
    List(
      TextTriplePattern("?X", s"$rdf#type", s"$ub#Course"),
      TextTriplePattern("?X", s"$ub#name", "?Y")))
  val lubmQuery3Patterns = builder.build(
    List(
      TextTriplePattern("?X", s"$ub#undergraduateDegreeFrom", "?Y"),
      TextTriplePattern("?X", s"$rdf#type", s"$ub#UndergraduateStudent"),
      TextTriplePattern("?X", s"$ub#memberOf", "?Z"),
      TextTriplePattern("?Z", s"$ub#subOrganizationOf", "?Y"),
      TextTriplePattern("?Z", s"$rdf#type", s"$ub#Department"),
      TextTriplePattern("?Y", s"$rdf#type", s"$ub#University")))
  val lubmQuery4Patterns = builder.build(
    List(
      TextTriplePattern("?X", s"$ub#worksFor", "http://www.Department0.University0.edu"),
      TextTriplePattern("?X", s"$rdf#type", s"$ub#FullProfessor"),
      TextTriplePattern("?X", s"$ub#name", "?Y1"),
      TextTriplePattern("?X", s"$ub#emailAddress", "?Y2"),
      TextTriplePattern("?X", s"$ub#telephone", "?Y3")))
  val lubmQuery5Patterns = builder.build(
    List(
      TextTriplePattern("?X", s"$ub#subOrganizationOf", "http://www.Department0.University0.edu"),
      TextTriplePattern("?X", s"$rdf#type", s"$ub#ResearchGroup")))
  val lubmQuery6Patterns = builder.build(
    List(
      TextTriplePattern("?Y", s"$ub#subOrganizationOf", "http://www.University0.edu"),
      TextTriplePattern("?Y", s"$rdf#type", s"$ub#Department"),
      TextTriplePattern("?X", s"$ub#worksFor", "?Y"),
      TextTriplePattern("?X", s"$rdf#type", s"$ub#FullProfessor")))
  val lubmQuery7Patterns = builder.build(
    List(
      TextTriplePattern("?Y", s"$rdf#type", s"$ub#FullProfessor"),
      TextTriplePattern("?Y", s"$ub#teacherOf", "?Z"),
      TextTriplePattern("?Z", s"$rdf#type", s"$ub#Course"),
      TextTriplePattern("?X", s"$ub#advisor", "?Y"),
      TextTriplePattern("?X", s"$ub#takesCourse", "?Z"),
      TextTriplePattern("?X", s"$rdf#type", s"$ub#UndergraduateStudent")))
      
//            SELECT ? "X" ? "Y" WHERE (
//        | - "X" - s"$rdf#type" - s"$ub#Course",
//        | - "X" - s"$ub#name" - "Y"),
//      SELECT ? "X" ? "Y" ? "Z" WHERE (
//        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
//        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent",
//        | - "X" - s"$ub#memberOf" - "Z",
//        | - "Z" - s"$ub#subOrganizationOf" - "Y",
//        | - "Z" - s"$rdf#type" - s"$ub#Department",
//        | - "Y" - s"$rdf#type" - s"$ub#University"),
//      SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
//        | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
//        | - "X" - s"$rdf#type" - s"$ub#FullProfessor",
//        | - "X" - s"$ub#name" - "Y1",
//        | - "X" - s"$ub#emailAddress" - "Y2",
//        | - "X" - s"$ub#telephone" - "Y3"),
//      SELECT ? "X" WHERE (
//        | - "X" - s"$ub#subOrganizationOf" - "http://www.Department0.University0.edu",
//        | - "X" - s"$rdf#type" - s"$ub#ResearchGroup"),
//      SELECT ? "X" ? "Y" WHERE (
//        | - "Y" - s"$ub#subOrganizationOf" - "http://www.University0.edu",
//        | - "Y" - s"$rdf#type" - s"$ub#Department",
//        | - "X" - s"$ub#worksFor" - "Y",
//        | - "X" - s"$rdf#type" - s"$ub#FullProfessor"),
//      SELECT ? "X" ? "Y" ? "Z" WHERE (
//        | - "Y" - s"$rdf#type" - s"$ub#FullProfessor",
//        | - "Y" - s"$ub#teacherOf" - "Z",
//        | - "Z" - s"$rdf#type" - s"$ub#Course",
//        | - "X" - s"$ub#advisor" - "Y",
//        | - "X" - s"$ub#takesCourse" - "Z",
//        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent"))

  println(lubmQuery1Patterns)
  println(lubmQuery2Patterns)
  println(lubmQuery3Patterns)
  println(lubmQuery4Patterns)
  println(lubmQuery5Patterns)
  println(lubmQuery6Patterns)
  println(lubmQuery7Patterns)
}
//
//  final val ISO8859: Codec = new Codec(Charset forName "ISO-8859-1")
//  final val UTF8: Codec    = new Codec(Charset forName "UTF-8")

class PatternBuilder(val dictionaryPath: String) {
  val dictionary = new HashMap[String, Int]()
  //implicit val codec = Codec.ISO8859
  val dictionaryFile = Source.fromFile(dictionaryPath, "UTF-16")
  var linesRead = 0
  for (line <- dictionaryFile.getLines) {
    val entry = line.split(" ")
    if (entry.length == 3) {
      dictionary.put(entry(0), Integer.parseInt(entry(2)))
      linesRead += 1
      if (linesRead % 10000 == 0) {
        println(s"$linesRead dictionary entries loaded.")
      }
    } else if (entry.length != 0) {
      throw new Exception(s"Failed to parse line $line, was parsed to ${entry.toList}.")
    }
  }

  def build(textPatterns: List[TextTriplePattern]): List[TriplePattern] = {
    var nextVariableId = -1
    var variables = Map[String, Int]()
    textPatterns map { textPattern =>
      def getId(entry: String): Int = {
        if (entry.startsWith("?")) {
          if (variables.contains(entry)) {
            variables(entry)
          } else {
            val id = nextVariableId
            variables += entry -> id
            nextVariableId -= 1
            id
          }
        } else {
          if (dictionary.containsKey(entry)) {
            dictionary.get(entry)
          } else {
            Int.MaxValue
          }
        }
      }
      val sId = getId(textPattern.s)
      val pId = getId(textPattern.p)
      val oId = getId(textPattern.o)
      TriplePattern(sId, pId, oId)
    }
  }

}