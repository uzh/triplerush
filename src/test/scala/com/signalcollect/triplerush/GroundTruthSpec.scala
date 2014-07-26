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

package com.signalcollect.triplerush

import scala.collection.immutable.TreeMap
import scala.concurrent.Await
import scala.io.Source
import scala.concurrent.duration.DurationInt
import org.junit.runner.RunWith
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner
import com.signalcollect.GraphBuilder
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.triplerush.QueryParticle._
import com.signalcollect.triplerush.optimizers.CleverCardinalityOptimizer
import com.signalcollect.triplerush.optimizers.PredicateSelectivityEdgeCountsOptimizer
import com.signalcollect.triplerush.optimizers.ExplorationOptimizer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import java.io.File
import com.signalcollect.triplerush.sparql.Sparql

object Lubm {

  def load(qe: TripleRush) {
    println("Loading LUBM1 ... ")
    for (fileNumber <- 0 to 14) {
      val filename = s".${File.separator}lubm${File.separator}university0_$fileNumber.nt"
      qe.loadNtriples(filename)
    }
    qe.prepareExecution
    println("Finished loading LUBM1.")
  }

  val sparqlQueries = List(
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X	
WHERE
{?X rdf:type ub:GraduateStudent .
  ?X ub:takesCourse "http://www.Department0.University0.edu/GraduateCourse0"}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE
{?X rdf:type ub:GraduateStudent .
  ?Y rdf:type ub:University .
  ?Z rdf:type ub:Department .
  ?X ub:memberOf ?Z .
  ?Z ub:subOrganizationOf ?Y .
  ?X ub:undergraduateDegreeFrom ?Y}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:Publication .
  ?X ub:publicationAuthor 
        <http://www.Department0.University0.edu/AssistantProfessor0>}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y1 ?Y2 ?Y3
WHERE
{
  ?X ub:worksFor <http://www.Department0.University0.edu> .
        ?X rdf:type ub:Professor .
  ?X ub:name ?Y1 .
  ?X ub:emailAddress ?Y2 .
  ?X ub:telephone ?Y3}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:Person .
  ?X ub:memberOf <http://www.Department0.University0.edu>}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X WHERE {?X rdf:type ub:Student}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X, ?Y
WHERE 
{?X rdf:type ub:Student .
  ?Y rdf:type ub:Course .
  ?X ub:takesCourse ?Y .
  <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X, ?Y, ?Z
WHERE
{?X rdf:type ub:Student .
  ?Y rdf:type ub:Department .
  ?X ub:memberOf ?Y .
  ?Y ub:subOrganizationOf <http://www.University0.edu> .
  ?X ub:emailAddress ?Z}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X, ?Y, ?Z
WHERE
{?X rdf:type ub:Student .
  ?Y rdf:type ub:Faculty .
  ?Z rdf:type ub:Course .
  ?X ub:advisor ?Y .
  ?Y ub:teacherOf ?Z .
  ?X ub:takesCourse ?Z}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:Student .
  ?X ub:takesCourse
<http://www.Department0.University0.edu/GraduateCourse0>}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:ResearchGroup .
  ?X ub:subOrganizationOf <http://www.University0.edu>}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X, ?Y
WHERE
{?X rdf:type ub:Chair .
  ?Y rdf:type ub:Department .
  ?X ub:worksFor ?Y .
  ?Y ub:subOrganizationOf <http://www.University0.edu>}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:Person .
  <http://www.University0.edu> ub:hasAlumnus ?X}
""",
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {?X rdf:type ub:UndergraduateStudent}
""")

}

@RunWith(classOf[JUnitRunner])
class GroundTruthSpec extends FlatSpec with Matchers with TestAnnouncements {

  val enabledQueries = Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)

  "LUBM Query 1" should "match the reference results" in {
    runTest(1)
  }

  "LUBM Query 2" should "match the reference results" in {
    runTest(2)
  }

  "LUBM Query 3" should "match the reference results" in {
    runTest(3)
  }

  "LUBM Query 4" should "match the reference results" in {
    runTest(4)
  }

  "LUBM Query 5" should "match the reference results" in {
    runTest(5)
  }

  "LUBM Query 6" should "match the reference results" in {
    runTest(6)
  }

  "LUBM Query 7" should "match the reference results" in {
    runTest(7)
  }

  "LUBM Query 8" should "match the reference results" in {
    runTest(8)
  }

  "LUBM Query 9" should "match the reference results" in {
    runTest(9)
  }

  "LUBM Query 10" should "match the reference results" in {
    runTest(10)
  }

  "LUBM Query 11" should "match the reference results" in {
    runTest(11)
  }

  "LUBM Query 12" should "match the reference results" in {
    runTest(12)
  }

  "LUBM Query 13" should "match the reference results" in {
    runTest(13)
  }

  "LUBM Query 14" should "match the reference results" in {
    runTest(14)
  }

  implicit val tr = new TripleRush
  Lubm.load(tr)

  override def afterAll {
    tr.shutdown
    super.afterAll
  }

  def executeOnQueryEngine(q: String): List[Bindings] = {
    val sparql = Sparql(q).get
    val bindings = sparql.resultIterator
    val bindingsList = bindings.map { binding =>
      val bindingsMap = sparql.selectVariableIds.map(
        variableId => sparql.idToVariableName(math.abs(variableId) - 1)).map(variable => (variable, binding(variable))).toMap
      bindingsMap
    }.toList
    val sortedBindings: List[TreeMap[String, String]] = bindingsList.
      map(unsortedBindings => TreeMap(unsortedBindings.toArray: _*))
    val sortedBindingList = (sortedBindings sortBy (map => map.values)).toList
    sortedBindingList
  }

  def bindingsToMap(bindings: Array[Int]): Map[Int, Int] = {
    (((-1 to -bindings.length by -1).zip(bindings))).toMap
  }

  type Bindings = TreeMap[String, String]
  type QuerySolution = List[Bindings]

  def runTest(queryId: Int) {
    if (enabledQueries.contains(queryId)) {
      val referenceResult = referenceResults(queryId)
      val ourResult = executeOnQueryEngine(Lubm.sparqlQueries(queryId - 1))
      assert(ourResult === referenceResult, s"TR result $ourResult for query $queryId did not match reference result $referenceResult.")
    }
  }

  val queryBaseName = s".${File.separator}answers${File.separator}answers_query"
  val referenceFiles: Map[Int, String] = ((1 to 14) map (queryNumber => queryNumber -> (queryBaseName + queryNumber + ".txt"))).toMap
  val referenceResults: Map[Int, QuerySolution] = {
    referenceFiles map { entry =>
      val fileName = entry._2
      val file = Source.fromFile(fileName)
      val lines = file.getLines
      val bindings = getQueryBindings(lines)
      (entry._1, bindings)
    }
  }

  def getQueryBindings(lines: Iterator[String]): QuerySolution = {
    var currentLine = lines.next
    if (currentLine == "NO ANSWERS.") {
      // No bindings.
      List()
    } else {
      val variables = currentLine.split("\t").toIndexedSeq
      var solution = List[Bindings]()
      while (lines.hasNext) {
        var binding = TreeMap[String, String]()
        currentLine = lines.next
        val values = currentLine.split("\t").toIndexedSeq
        for (i <- 0 until variables.size) {
          binding += variables(i) -> values(i)
        }
        solution = binding :: solution
      }
      solution.sortBy(map => map.values)
    }
  }

}
