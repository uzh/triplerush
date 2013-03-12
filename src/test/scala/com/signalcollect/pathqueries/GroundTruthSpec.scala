package com.signalcollect.pathqueries

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import SparqlDsl._
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.rdf.model.Model

@RunWith(classOf[JUnitRunner])
class GroundTruthSpec extends SpecificationWithJUnit {

  sequential

  "LUBM Query 1" should {
    val query1Dsl = select ? "x" where (
      | - "x" - s"$ub#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
      | - "x" - s"$rdf#type" - s"$ub#GraduateStudent")

    val query1Sparql = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>    
SELECT ?x WHERE { 
  ?x rdf:type ub:GraduateStudent . 
  ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> 
}
"""
    "produce the same results in Jena" in {
      1 must beBetween(0, 2)
    }
  }

  "LUBM Query 2" should {
    val query1Dsl = select ? "x" ? "y" ? "z" where (
      | - "x" - s"$rdf#type" - s"$ub#GraduateStudent",
      | - "x" - s"$ub#memberOf" - "z",
      | - "z" - s"$rdf#type" - s"$ub#Department",
      | - "z" - s"$ub#subOrganizationOf" - "y",
      | - "x" - s"$ub#undergraduateDegreeFrom" - "y",
      | - "y" - s"$rdf#type" - s"$ub#University")

    val query1Sparql = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>    
SELECT ?x ?y ?z WHERE { 
	?x rdf:type ub:GraduateStudent . 
	?y rdf:type ub:University . 
	?z rdf:type ub:Department . 
	?x ub:memberOf ?z . 
	?z ub:subOrganizationOf ?y . 
	?x ub:undergraduateDegreeFrom ?y
}
"""
    "produce the same results in Jena" in {
      1 must beBetween(0, 2)
    }
  }

  """

# query 2
SELECT ?x ?y ?z WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent> . 
	?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University> . 
	?z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department> . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf> ?z . 
	?z <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf> ?y . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom> ?y
}

# query 3
SELECT ?x WHERE { 
	?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication> . 
	?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor> <http://www.Department0.University0.edu/AssistantProfessor0> 
}

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