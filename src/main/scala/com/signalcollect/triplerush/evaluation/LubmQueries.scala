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

import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.TriplePattern

object QueryEncoding {
  def apply(id: String) = m(id)

  def ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
  def rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
//http://www.w3.org/1999/02/22-rdf-syntax-ns#type
  def m = Map(
    (s"$rdf#type", 1),
    (s"$ub#GraduateStudent", 2),
    (s"$ub#undergraduateDegreeFrom", 3),
    (s"$ub#memberOf", 4),
    (s"$ub#Department", 5),
    (s"$ub#subOrganizationOf", 6),
    (s"$ub#University", 7),
    (s"$ub#Course", 8),
    (s"$ub#name", 9),
    (s"$ub#UndergraduateStudent", 10),
    (s"$ub#worksFor", 11),
    (s"http://www.Department0.University0.edu", 12),
    (s"$ub#FullProfessor", 13),
    (s"$ub#emailAddress", 14),
    (s"$ub#telephone", 15),
    (s"$ub#ResearchGroup", 16),
    (s"http://www.University0.edu", 17),
    (s"$ub#teacherOf", 18),
    (s"$ub#advisor", 19),
    (s"$ub#takesCourse", 20))
//    (s"http://www.w3.org/2004/02/skos/core#subject", 21),
//    (s"http://dbpedia.org/resource/Category:First-person_shooters", 22),
//       (s"$foaf:name", 23),
//    (s"$foaf:homepage", 24),
//    (s"rdf#type", 25),
//    (s"http://dbpedia.org/resource/Category:German_musicians", 26),
//    (s"$rdfs#comment", 27),
//    (s"$dbo:birthPlace", 28),
//    (s"http://dbpedia.org/resource/Berlin", 29),
//    (s"$dbo:birthDate", 30),
//    (s"$dbo:deathDate", 31),
//    (s"http://dbpedia.org/resource/Category:Luxury_vehicles", 32),
//    (s"$dbo:manufacturer", 33),
//    (s"$dbprop:name", 34),
//    (s"$dbprop:pages", 35),
//    (s"$dbprop:isbn", 36),
//    (s"$dbprop:author", 37),
//    (s"$foaf:page", 38),
//    (s"$dbo:SoccerPlayer", 39),
//    (s"$dbprop:position", 40),
//    (s"$dbprop:clubs", 41),
//    (s"$dbo:capacity", 42),
//    (s"$dbprop:population", 43),
//    (s"$dbo:number", 44)
}

object LubmQueries {

  /**
   * Queries from: http://www.cs.rpi.edu/~zaki/PaperDir/WWW10.pdf
   * Result sizes from: http://research.microsoft.com/pubs/183717/Trinity.RDF.pdf
   *            L1   L2       L3 L4 L5 L6  L7
   * LUBM-160   397  173040   0  10 10 125 7125
   * LUBM-10240 2502 11016920 0  10 10 125 450721
   *
   * Times Trinity: 281 132 110  5    4 9 630
   */
  val x = -1
  val y = -2
  val z = -3

  import QueryEncoding._
  
  def fullQueries: List[QuerySpecification] = List(
    QuerySpecification(List(
      TriplePattern(x, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#GraduateStudent")), // ?X rdf:type ub:GraduateStudent
      TriplePattern(x, QueryEncoding(s"$ub#undergraduateDegreeFrom"), y), // ?X ub:undergraduateDegreeFrom ?Y
      TriplePattern(x, QueryEncoding(s"$ub#memberOf"), z), // ?X ub:memberOf ?Z
      TriplePattern(z, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#Department")), // ?Z rdf:type ub:Department
      TriplePattern(z, QueryEncoding(s"$ub#subOrganizationOf"), y), // ?Z ub:subOrganizationOf ?Y
      TriplePattern(y, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#University")) // ?Y rdf:type ub:University
      )),
    QuerySpecification(List(
      TriplePattern(x, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#Course")), // ?X rdf:type ub:Course
      TriplePattern(x, QueryEncoding(s"$ub#name"), y) // ?X ub:name ?Y),
      )),
    QuerySpecification(List(
      TriplePattern(x, QueryEncoding(s"$ub#undergraduateDegreeFrom"), y), // ?X ub:undergraduateDegreeFrom ?Y
      TriplePattern(x, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#UndergraduateStudent")), // ?X rdf:type ub:UndergraduateStudent
      TriplePattern(x, QueryEncoding(s"$ub#memberOf"), z), // ?X ub:memberOf ?Z
      TriplePattern(z, QueryEncoding(s"$ub#subOrganizationOf"), y), // ?Z ub:subOrganizationOf ?Y
      TriplePattern(z, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#Department")), // ?Z rdf:type ub:Department
      TriplePattern(y, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#University")) // ?Y rdf:type ub:University
      )),
    QuerySpecification(List(
      TriplePattern(x, QueryEncoding(s"$ub#worksFor"), QueryEncoding("http://www.Department0.University0.edu")), // ?X ub:worksFor http://www.Department0.University0.edu
      TriplePattern(x, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#FullProfessor")), // ?X rdf:type ub:FullProfessor
      TriplePattern(x, QueryEncoding(s"$ub#name"), y), // ?X ub:name ?Y1
      TriplePattern(x, QueryEncoding(s"$ub#emailAddress"), z), // ?X ub:emailAddress ?Y2
      TriplePattern(x, QueryEncoding(s"$ub#telephone"), -4) // ?X ub:telephone ?Y3
      )),
    QuerySpecification(List(
      TriplePattern(x, QueryEncoding(s"$ub#subOrganizationOf"), QueryEncoding("http://www.Department0.University0.edu")), // ?X ub:subOrganizationOf http://www.Department0.University0.edu
      TriplePattern(x, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#ResearchGroup")) // ?X rdf:type ub:ResearchGroup
      )),
    QuerySpecification(List(
      TriplePattern(y, QueryEncoding(s"$ub#subOrganizationOf"), QueryEncoding("http://www.University0.edu")), // ?Y ub:subOrganizationOf http://www.University0.edu
      TriplePattern(y, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#Department")), //?Y rdf:type ub:Department
      TriplePattern(x, QueryEncoding(s"$ub#worksFor"), y), // ?X ub:worksFor ?Y
      TriplePattern(x, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#FullProfessor")) // ?X rdf:type ub:FullProfessor
      )),
    QuerySpecification(List(
      TriplePattern(y, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#FullProfessor")), // ?Y rdf:type ub:FullProfessor
      TriplePattern(y, QueryEncoding(s"$ub#teacherOf"), z), // ?Y ub:teacherOf ?Z
      TriplePattern(z, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#Course")), // ?Z rdf:type ub:Course
      TriplePattern(x, QueryEncoding(s"$ub#advisor"), y), // ?X ub:advisor ?Y
      TriplePattern(x, QueryEncoding(s"$ub#takesCourse"), z), // ?X ub:takesCourse ?Z
      TriplePattern(x, QueryEncoding(s"$rdf#type"), QueryEncoding(s"$ub#UndergraduateStudent")) // ?X rdf:type ub:UndergraduateStudent
      )))

}