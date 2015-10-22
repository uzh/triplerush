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

import org.apache.jena.riot.Lang
import scala.collection.immutable.TreeMap
import scala.concurrent.Await
import scala.io.Source
import scala.concurrent.duration.DurationInt
import com.signalcollect.GraphBuilder
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.triplerush.QueryParticle._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import java.io.{ FileInputStream, File }
import com.signalcollect.triplerush.sparql.Sparql
import com.signalcollect.triplerush.sparql.TripleRushGraph
import collection.JavaConversions._
import com.signalcollect.triplerush.loading.TripleIterator

object Lubm {

  def load(qe: TripleRush, toId: Int = 14) {
    println("Loading LUBM1 ... ")
    for (fileNumber <- 0 to toId) {
      val resource = s"university0_$fileNumber.nt"
      val tripleStream = getClass.getResourceAsStream(resource)
      println(s"Loading file $resource ...")
      qe.addTriples(TripleIterator(tripleStream, Lang.NTRIPLES))
      println(s"Done loading $resource.")
    }
    println("Finished loading LUBM1.")
  }

  val sparqlQueries = List(
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> .
  ?X rdf:type ub:GraduateStudent .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Z rdf:type ub:Department .
  ?Z ub:subOrganizationOf ?Y .
  ?Y rdf:type ub:University .
  ?X ub:undergraduateDegreeFrom ?Y .
  ?X ub:memberOf ?Z .
  ?X rdf:type ub:GraduateStudent .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor0> .
  ?X rdf:type ub:Publication .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y1 ?Y2 ?Y3
WHERE {
  ?X ub:worksFor <http://www.Department0.University0.edu> .
        ?X rdf:type ub:Professor .
  ?X ub:name ?Y1 .
  ?X ub:emailAddress ?Y2 .
  ?X ub:telephone ?Y3 .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:memberOf <http://www.Department0.University0.edu> .
  ?X rdf:type ub:Person .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X WHERE {?X rdf:type ub:Student}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y
WHERE {
  <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y .
  ?Y rdf:type ub:Course .
  ?X ub:takesCourse ?Y .
  ?X rdf:type ub:Student .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Y ub:subOrganizationOf <http://www.University0.edu> .
  ?Y rdf:type ub:Department .
  ?X ub:memberOf ?Y .
  ?X rdf:type ub:Student .
  ?X ub:emailAddress ?Z .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Y rdf:type ub:Faculty .
  ?Y ub:teacherOf ?Z .
  ?X ub:advisor ?Y .
  ?X ub:takesCourse ?Z .
  ?Z rdf:type ub:Course .
  ?X rdf:type ub:Student .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> .
  ?X rdf:type ub:Student .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:subOrganizationOf <http://www.University0.edu> .
  ?X rdf:type ub:ResearchGroup .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y
WHERE {
  ?Y ub:subOrganizationOf <http://www.University0.edu> .
  ?Y rdf:type ub:Department .
  ?X ub:worksFor ?Y .
  ?X rdf:type ub:Chair .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X WHERE {
  <http://www.University0.edu> ub:hasAlumnus ?X .
  ?X rdf:type ub:Person .
}
    """,
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X rdf:type ub:UndergraduateStudent .
}
    """)

}

class GroundTruthSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

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

  var tr: TripleRush = _
  lazy val graph = TripleRushGraph(tr)
  implicit lazy val model = graph.getModel

  override def beforeAll {
    tr = TestStore.instantiateUniqueStore()
    Lubm.load(tr)
  }

  override def afterAll {
    tr.shutdown
  }

  def executeOnQueryEngine(q: String): List[Bindings] = {
    val bindings = Sparql(q)
    val bindingsList = bindings.map { binding =>
      val bindingsMap = bindings.getResultVars.map(
        variable => (variable, binding.get(variable).toString)).toMap
      bindingsMap
    }.toList
    val sortedBindings: List[TreeMap[String, String]] = bindingsList.
      map(unsortedBindings => TreeMap(unsortedBindings.toArray: _*))
    val sortedBindingList = (sortedBindings.sortBy(map => map.values)).toList
    sortedBindingList
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

  val queryBaseName = s"answers_query"
  val answerResources = (1 to 14).map { queryNumber =>
    queryNumber -> getClass.getResource(queryBaseName + queryNumber + ".txt")
  }.toMap
  val referenceResults: Map[Int, QuerySolution] = {
    answerResources map { entry =>
      val resource = entry._2
      val source = Source.fromURL(resource)
      val lines = source.getLines
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
