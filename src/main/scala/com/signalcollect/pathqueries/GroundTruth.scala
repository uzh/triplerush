package com.signalcollect.pathqueries

import com.hp.hpl.jena.rdf.model.Model
import java.io.InputStream
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import collection.JavaConversions._
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QueryExecutionFactory

object GroundTruth extends App {
  val fileName = "./uni0-0.nt"
  val model = ModelFactory.createDefaultModel
  val is = FileManager.get.open(fileName)
  if (is != null) {
    model.read(is, null, "N-TRIPLE")
  } else {
    System.err.println("cannot read " + fileName)
  }

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
  val query = QueryFactory.create(queryString)
  val qexec = QueryExecutionFactory.create(query, model)
  try {
    val results = qexec.execSelect
    for (result <- results) {
      val x = result.get("x")
      println(s"x -> $x")
    }
  } finally {
    qexec.close
  }

  //  for (rdfThing <- model.listStatements) {
  //    println(rdfThing)
  //  }

}