package com.signalcollect.triplerush

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import SparqlDsl._
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.rdf.model.Model
import com.signalcollect.triplerush.SparqlDsl._

import scala.io.Source
import scala.io.Codec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

@RunWith(classOf[JUnitRunner])
class GroundTruthSpec extends SpecificationWithJUnit {

  sequential

  "LUBM Query 1" should {

    //    val query1 = SELECT ? "X" WHERE (
    //      | - "X" - s"$ub#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
    //      | - "X" - s"$rdf#type" - s"$ub#GraduateStudent")

    "match the reference results 1" in {
      val referenceResult = referenceResults(1)
      val ourResult = executeOnQueryEngine(q1)
      ourResult === referenceResult
    }
  }

  "LUBM Query 2" should {

    val query2 = SELECT ? "X" ? "Y" ? "Z" WHERE (
      | - "X" - s"$rdf#type" - s"$ub#GraduateStudent",
      | - "X" - s"$ub#memberOf" - "Z",
      | - "Z" - s"$rdf#type" - s"$ub#Department",
      | - "Z" - s"$ub#subOrganizationOf" - "Y",
      | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
      | - "Y" - s"$rdf#type" - s"$ub#University")

    "match the reference results 2" in {
      val referenceResult = referenceResults(2)
      val ourResult = executeOnQueryEngine(query2)
      ourResult === referenceResult
    }
  }

  "LUBM Query 3" should {

    val query3 = SELECT ? "X" WHERE (
      | - "X" - s"$ub#publicationAuthor" - "http://www.Department0.University0.edu/AssistantProfessor0",
      | - "X" - s"$rdf#type" - s"$ub#Publication")

    "match the reference results in 3" in {
      val referenceResult = referenceResults(3)
      val ourResult = executeOnQueryEngine(query3)
      ourResult === referenceResult
    }
  }

  "LUBM Query 4" should {

    //    val query4 = SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
    //      | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
    //      | - "X" - s"$rdf#type" - s"$ub#Professor",
    //      | - "X" - s"$ub#name" - "Y1",
    //      | - "X" - s"$ub#emailAddress" - "Y2",
    //      | - "X" - s"$ub#telephone" - "Y3")

    val query4A = SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
      | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
      | - "X" - s"$rdf#type" - s"$ub#AssistantProfessor",
      | - "X" - s"$ub#name" - "Y1",
      | - "X" - s"$ub#emailAddress" - "Y2",
      | - "X" - s"$ub#telephone" - "Y3")

    val query4B = SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
      | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
      | - "X" - s"$rdf#type" - s"$ub#AssociateProfessor",
      | - "X" - s"$ub#name" - "Y1",
      | - "X" - s"$ub#emailAddress" - "Y2",
      | - "X" - s"$ub#telephone" - "Y3")

    val query4C = SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
      | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
      | - "X" - s"$rdf#type" - s"$ub#FullProfessor",
      | - "X" - s"$ub#name" - "Y1",
      | - "X" - s"$ub#emailAddress" - "Y2",
      | - "X" - s"$ub#telephone" - "Y3")

    "match the reference results in 4" in {
      val referenceResult = referenceResults(4)
      val a = executeOnQueryEngine(query4A)
      val b = executeOnQueryEngine(query4B)
      val c = executeOnQueryEngine(query4C)
      val ourResult = (a ::: b ::: c).sortBy(map => map.values)
      println("ref: " + referenceResult)
      println("our: " + ourResult)
      ourResult === referenceResult
    }
  }

  //  "LUBM Query 5" should {
  //
  //    val query5 = SELECT ? "X" WHERE (
  //      | - "X" - s"$ub#memberOf" - "http://www.Department0.University0.edu",
  //      | - "X" - s"$rdf#type" - s"$ub#Person")
  //
  //    "match the reference results in 5" in {
  //      val referenceResult = referenceResults(5)
  //      val ourResult = executeOnQueryEngine(query5)
  //      ourResult === referenceResult
  //    }
  //  }

  //  "LUBM Query 6" should {
  //
  //    val query6 = SELECT ? "X" WHERE (
  //      | - "X" - s"$ub#type" - s"$ub#Student")
  //
  //    "match the reference results in 6" in {
  //      val referenceResult = referenceResults(6)
  //      val ourResult = executeOnQueryEngine(query6)
  //      ourResult === referenceResult
  //    }
  //  }

  "LUBM Query 7" should {

    val query7 = SELECT ? "X" WHERE (
      | - "X" - s"$ub#type" - s"$ub#Student")

    "match the reference results in 7" in {
      val referenceResult = referenceResults(7)
      val ourResult = executeOnQueryEngine(query7)
      ourResult === referenceResult
    }
  }

  val query1String = """
# Query1
# This query bears large input and high selectivity. It queries about just one class and
# one property and does not assume any hierarchy information or inference.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X	
WHERE
{?X rdf:type ub:GraduateStudent .
  ?X ub:takesCourse
"http://www.Department0.University0.edu/GraduateCourse0"}
"""

  val q1 = PatternQuery.build(query1String) match {
    case Left(q) =>
      println(q)
      q
    case Right(error) =>
      println(error)
      throw new Exception(error)
  }

  val others = """
# Query1
# This query bears large input and high selectivity. It queries about just one class and
# one property and does not assume any hierarchy information or inference.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X	
WHERE
{?X rdf:type ub:GraduateStudent .
  ?X ub:takesCourse
http://www.Department0.University0.edu/GraduateCourse0}

# Query2
# This query increases in complexity: 3 classes and 3 properties are involved. Additionally, 
# there is a triangular pattern of relationships between the objects involved.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X, ?Y, ?Z
WHERE
{?X rdf:type ub:GraduateStudent .
  ?Y rdf:type ub:University .
  ?Z rdf:type ub:Department .
  ?X ub:memberOf ?Z .
  ?Z ub:subOrganizationOf ?Y .
  ?X ub:undergraduateDegreeFrom ?Y}

# Query3
# This query is similar to Query 1 but class Publication has a wide hierarchy.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:Publication .
  ?X ub:publicationAuthor 
        http://www.Department0.University0.edu/AssistantProfessor0}

# Query4
# This query has small input and high selectivity. It assumes subClassOf relationship 
# between Professor and its subclasses. Class Professor has a wide hierarchy. Another 
# feature is that it queries about multiple properties of a single class.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X, ?Y1, ?Y2, ?Y3
WHERE
{?X rdf:type ub:Professor .
  ?X ub:worksFor <http://www.Department0.University0.edu> .
  ?X ub:name ?Y1 .
  ?X ub:emailAddress ?Y2 .
  ?X ub:telephone ?Y3}

# Query5
# This query assumes subClassOf relationship between Person and its subclasses
# and subPropertyOf relationship between memberOf and its subproperties.
# Moreover, class Person features a deep and wide hierarchy.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:Person .
  ?X ub:memberOf <http://www.Department0.University0.edu>}


# Query6
# This query queries about only one class. But it assumes both the explicit
# subClassOf relationship between UndergraduateStudent and Student and the
# implicit one between GraduateStudent and Student. In addition, it has large
# input and low selectivity.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X WHERE {?X rdf:type ub:Student}


# Query7
# This query is similar to Query 6 in terms of class Student but it increases in the
# number of classes and properties and its selectivity is high.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X, ?Y
WHERE 
{?X rdf:type ub:Student .
  ?Y rdf:type ub:Course .
  ?X ub:takesCourse ?Y .
  <http://www.Department0.University0.edu/AssociateProfessor0>,   
  	ub:teacherOf, ?Y}


# Query8
# This query is further more complex than Query 7 by including one more property.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X, ?Y, ?Z
WHERE
{?X rdf:type ub:Student .
  ?Y rdf:type ub:Department .
  ?X ub:memberOf ?Y .
  ?Y ub:subOrganizationOf <http://www.University0.edu> .
  ?X ub:emailAddress ?Z}


# Query9
# Besides the aforementioned features of class Student and the wide hierarchy of
# class Faculty, like Query 2, this query is characterized by the most classes and
# properties in the query set and there is a triangular pattern of relationships.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X, ?Y, ?Z
WHERE
{?X rdf:type ub:Student .
  ?Y rdf:type ub:Faculty .
  ?Z rdf:type ub:Course .
  ?X ub:advisor ?Y .
  ?Y ub:teacherOf ?Z .
  ?X ub:takesCourse ?Z}


# Query10
# This query differs from Query 6, 7, 8 and 9 in that it only requires the
# (implicit) subClassOf relationship between GraduateStudent and Student, i.e., 
#subClassOf rela-tionship between UndergraduateStudent and Student does not add
# to the results.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:Student .
  ?X ub:takesCourse
<http://www.Department0.University0.edu/GraduateCourse0>}


# Query11
# Query 11, 12 and 13 are intended to verify the presence of certain OWL reasoning
# capabilities in the system. In this query, property subOrganizationOf is defined
# as transitive. Since in the benchmark data, instances of ResearchGroup are stated
# as a sub-organization of a Department individual and the later suborganization of 
# a University individual, inference about the subOrgnizationOf relationship between
# instances of ResearchGroup and University is required to answer this query. 
# Additionally, its input is small.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:ResearchGroup .
  ?X ub:subOrganizationOf <http://www.University0.edu>}


# Query12
# The benchmark data do not produce any instances of class Chair. Instead, each
# Department individual is linked to the chair professor of that department by 
# property headOf. Hence this query requires realization, i.e., inference that
# that professor is an instance of class Chair because he or she is the head of a
# department. Input of this query is small as well.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X, ?Y
WHERE
{?X rdf:type ub:Chair .
  ?Y rdf:type ub:Department .
  ?X ub:worksFor ?Y .
  ?Y ub:subOrganizationOf <http://www.University0.edu>}


# Query13
# Property hasAlumnus is defined in the benchmark ontology as the inverse of
# property degreeFrom, which has three subproperties: undergraduateDegreeFrom, 
# mastersDegreeFrom, and doctoralDegreeFrom. The benchmark data state a person as
# an alumnus of a university using one of these three subproperties instead of
# hasAlumnus. Therefore, this query assumes subPropertyOf relationships between 
# degreeFrom and its subproperties, and also requires inference about inverseOf.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X
WHERE
{?X rdf:type ub:Person .
  <http://www.University0.edu> ub:hasAlumnus ?X}


# Query14
# This query is the simplest in the test set. This query represents those with large input and low selectivity and does not assume any hierarchy information or inference.
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X
WHERE {?X rdf:type ub:UndergraduateStudent}


"""

  val qe = new QueryEngine
  for (fileNumber <- 0 to 14) {
    val filename = s"./uni0-$fileNumber.nt"
    print(s"loding $filename ...")
    qe.load(filename)
    println(" done")
  }

  val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
  val testData = "./uni0-0.nt"
  val jenaModel = ModelFactory.createDefaultModel

  def executeOnQueryEngine(q: PatternQuery): List[Bindings] = {
    val resultFuture = qe.executeQuery(q)
    val result = Await.result(resultFuture, new FiniteDuration(100, TimeUnit.SECONDS))
    val bindings = result map (_.bindings.map map (entry => (Mapping.getString(entry._1), Mapping.getString(entry._2))))
    val sortedBindings = bindings sortBy (map => map.values)
    sortedBindings
  }

  type Bindings = Map[String, String]
  type QuerySolution = List[Bindings]

  val queryBaseName = "./answers/answers_query"
  val referenceFiles: Map[Int, String] = ((1 to 14) map (queryNumber => queryNumber -> (queryBaseName + queryNumber + ".txt"))).toMap
  val referenceResults: Map[Int, QuerySolution] = {
    referenceFiles map { entry =>
      val fileName = entry._2
      val file = Source.fromFile(fileName)
      val lines = file.getLines
      val bindings = getBindings(lines)
      (entry._1, bindings)
    }
  }

  def getBindings(lines: Iterator[String]): QuerySolution = {
    var currentLine = lines.next
    if (currentLine == "NO ANSWERS.") {
      // No bindings.
      List()
    } else {
      val variables = currentLine.split("\t").toIndexedSeq
      var solution = List[Bindings]()
      while (lines.hasNext) {
        var binding = Map[String, String]()
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

  //  loadJenaModel(testData, jenaModel)
  //  def loadJenaModel(filename: String, model: Model) {
  //    val testFile = filename
  //    val is = FileManager.get.open(testFile)
  //    if (is != null) {
  //      model.read(is, null, "N-TRIPLE")
  //    } else {
  //      System.err.println("cannot read " + testFile)
  //    }
  //  }

}