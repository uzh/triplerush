package com.signalcollect.triplerush
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import collection.JavaConversions._
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import com.hp.hpl.jena.sparql.syntax.ElementMinus
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph
import com.hp.hpl.jena.sparql.syntax.ElementExists
import com.hp.hpl.jena.sparql.syntax.ElementAssign
import com.hp.hpl.jena.sparql.syntax.ElementFetch
import com.hp.hpl.jena.sparql.syntax.ElementDataset
import com.hp.hpl.jena.sparql.syntax.ElementBind
import com.hp.hpl.jena.sparql.syntax.ElementUnion
import com.hp.hpl.jena.sparql.syntax.ElementOptional
import com.hp.hpl.jena.sparql.syntax.ElementService
import com.hp.hpl.jena.sparql.syntax.ElementData
import com.hp.hpl.jena.sparql.syntax.ElementNotExists
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import collection.JavaConversions._
import com.hp.hpl.jena.sparql.syntax.ElementVisitor
import com.hp.hpl.jena.query.Query

object GroundTruth extends App {
  val model = ModelFactory.createDefaultModel

  for (fileNumber <- 0 to 14) {
    val filename = s"./uni0-$fileNumber.nt"
    print(s"loding $filename ...")
    val is = FileManager.get.open(filename)
    if (is != null) {
      model.read(is, null, "N-TRIPLE")
    } else {
      System.err.println("cannot read " + filename)
    }
    println(" done")
  }

  /*
    /**
   * # Query1
   * # This query bears large input and high selectivity. It queries about just one class and
   * # one property and does not assume any hierarchy information or inference.
   *  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
   *  PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
   *  SELECT ?X	WHERE
   *  {?X rdf:type ub:GraduateStudent .
   *   ?X ub:takesCourse http://www.Department0.University0.edu/GraduateCourse0}
   */
  val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"

  val lubm1 = select ? "X" where (
    | - "X" - s"$ub#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
    | - "X" - "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" - s"$ub#GraduateStudent")

  /**
   * # Query2
   * # This query increases in complexity: 3 classes and 3 properties are involved. Additionally,
   * # there is a triangular pattern of relationships between the objects involved.
   * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
   * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
   * SELECT ?X, ?Y, ?Z
   * WHERE
   * {?X rdf:type ub:GraduateStudent .
   * ?Y rdf:type ub:University .
   * ?Z rdf:type ub:Department .
   * ?X ub:memberOf ?Z .
   * ?Z ub:subOrganizationOf ?Y .
   * ?X ub:undergraduateDegreeFrom ?Y}
   */
  val lubm2 = select ? "X" ? "Y" ? "Z" where (
    | - "X" - "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" - "http://swat.cse.lehigh.edu/onto/univ-bench.owl#GraduateStudent",
    | - "X" - "http://swat.cse.lehigh.edu/onto/univ-bench.owl#memberOf" - "Z",
    | - "Z" - "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" - "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Department",
    | - "Z" - "http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf" - "Y",
    | - "X" - "http://swat.cse.lehigh.edu/onto/univ-bench.owl#undergraduateDegreeFrom" - "Y",
    | - "Y" - "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" - "http://swat.cse.lehigh.edu/onto/univ-bench.owl#University")*/

  //  PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
  //SELECT ?name ?mbox
  //WHERE
  //  { ?x foaf:name ?name .
  //    ?x foaf:mbox ?mbox }

  //    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
  //  
  //  val lubm1 = select ? "X" where (
  //    | - "X" - s"$ub#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
  //    | - "X" - "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" - s"$ub#GraduateStudent")

  val queryString = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>    
SELECT ?x WHERE { 
	?x rdf:type ub:GraduateStudent . 
	?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> 
}
"""
  val queryString2 = s"""
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>    
     SELECT ?x ?y ?z WHERE { 
	        ?x rdf:type ub:GraduateStudent . 
	        ?y rdf:type ub:University . 
	        ?z rdf:type ub:Department . 
	        ?x ub:memberOf ?z . 
	        ?z ub:subOrganizationOf ?y . 
	        ?x ub:undergraduateDegreeFrom ?y
     }"""

  val query: com.hp.hpl.jena.query.Query = QueryFactory.create(queryString2)

  val qexec = QueryExecutionFactory.create(query, model)
  try {
    val results = qexec.execSelect
    for (result <- results) {
      val x = result.get("x")
      println(s"x -> $x")
    }
    if (results.isEmpty) {
      println("No results for this query.")
    }
  } finally {
    qexec.close
  }

  //  for (rdfThing <- model.listStatements) {
  //    println(rdfThing)
  //  }

}