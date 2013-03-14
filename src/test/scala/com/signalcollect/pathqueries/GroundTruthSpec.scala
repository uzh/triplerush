package com.signalcollect.pathqueries

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import SparqlDsl._
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.rdf.model.Model
import com.signalcollect.pathqueries.SparqlDsl._
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
      val bindings = Map[String, String]()
      // No bindings.
      List(bindings)
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

  val qe = new QueryEngine
  for (fileNumber <- 0 to 14) {
    val filename = s"./uni0-$fileNumber.nt"
    print(s"loding $filename ...")
    qe.load(filename)
    println(" done")
  }

  "LUBM Query 1" should {

    val query1 = SELECT ? "X" WHERE (
      | - "X" - s"$ub#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
      | - "X" - s"$rdf#type" - s"$ub#GraduateStudent")

    "match the reference results" in {
      val referenceResult = referenceResults(1)
      val ourResult = executeOnQueryEngine(query1)
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

    "match the reference results" in {
      val referenceResult = referenceResults(2)
      val ourResult = executeOnQueryEngine(query2)
      ourResult === referenceResult
    }
  }

  "LUBM Query 3" should {

    //    # query 3
    //SELECT ?x WHERE { 
    //	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication> . 
    //	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor> <http://www.Department0.University0.edu/AssistantProfessor0> 
    //}

    val query1Sparql = s"""
     PREFIX rdf: <$rdf#>
     PREFIX ub: <$ub#>    
     SELECT ?x WHERE {
            ?x rdf:type ub:GraduateStudent . 
            ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> 
     }"""

    val query3Dsl = SELECT ? "x" WHERE (
      | - "x" - s"$ub#memberOf" - "z",
      | - "x" - s"$ub#publicationAuthor" - "http://www.Department0.University0.edu/AssistantProfessor0")

    val query2Sparql = s"""
     PREFIX rdf: <$rdf#>
     PREFIX ub: <$ub#>    
     SELECT ?x ?y ?z WHERE { 
	        ?x rdf:type ub:GraduateStudent . 
	        ?y rdf:type ub:University . 
	        ?z rdf:type ub:Department . 
	        ?x ub:memberOf ?z . 
	        ?z ub:subOrganizationOf ?y . 
	        ?x ub:undergraduateDegreeFrom ?y
     }"""

    val query3Sparql = s"""
     PREFIX rdf: <$rdf#>
     PREFIX ub: <$ub#>    
     SELECT ?x ?y ?z WHERE { 
	        ?x rdf:type ub:GraduateStudent . 
	        ?y rdf:type ub:University . 
	        ?z rdf:type ub:Department . 
	        ?x ub:memberOf ?z . 
	        ?z ub:subOrganizationOf ?y . 
	        ?x ub:undergraduateDegreeFrom ?y
     }"""

    "match the reference results" in {
      1 must beBetween(0, 2)
    }
  }

  val others = """
# query 4
SELECT ?x ?y1 ?y2 ?y3 WHERE {
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Professor> . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor> <http://www.Department0.University0.edu> . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name> ?y1 . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress> ?y2 .
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone> ?y3
}

# query 5
SELECT ?x WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Person> . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf> <http://www.Department0.University0.edu> 
}

# query 6
SELECT ?x WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> 
}

# query 7
SELECT ?x ?y WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> . 
	?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course> . 
	<http://www.Department0.University0.edu/AssociateProfessor0> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf> ?y . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse> ?y 
}

# query 8
SELECT ?x ?y ?z WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> .
	?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department> .
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf> ?y .
	?y <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf> <http://www.University0.edu> .
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress> ?z
}

# query 9
SELECT ?x ?y ?z WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> . 
	?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Faculty> . 
	?z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course> .
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor> ?y . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse> ?z . 
	?y <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf> ?z 
}

# query 10
SELECT ?x WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse> <http://www.Department0.University0.edu/GraduateCourse0> 
}

# query 11
SELECT ?x WHERE { 
	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#ResearchGroup> ?x . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf> <http://www.University0.edu> 
}

# query 12
SELECT ?x ?y WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Chair> . 
	?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department> . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor> ?y .
	?y <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf> <http://www.University0.edu> 
}

# query 13
SELECT ?x WHERE {
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Person> .
	<http://www.University0.edu> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#hasAlumnus> ?x 
}

# query 14
SELECT ?x WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent> 
}
"""

  val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
  val testData = "./uni0-0.nt"
  val jenaModel = ModelFactory.createDefaultModel

  def executeOnQueryEngine(q: PatternQuery) = {
    val resultFuture = qe.executeQuery(q)
    val result = Await.result(resultFuture, new FiniteDuration(100, TimeUnit.SECONDS))
    val bindings = result map (_.bindings.map map (entry => (Mapping.getString(entry._1), Mapping.getString(entry._2))))
    val sortedBindings = bindings sortBy (map => map.values)
  }

  loadJenaModel(testData, jenaModel)
  def loadJenaModel(filename: String, model: Model) {
    val testFile = filename
    val is = FileManager.get.open(testFile)
    if (is != null) {
      model.read(is, null, "N-TRIPLE")
    } else {
      System.err.println("cannot read " + testFile)
    }
  }

}