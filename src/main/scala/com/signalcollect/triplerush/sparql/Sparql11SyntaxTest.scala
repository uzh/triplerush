package com.signalcollect.triplerush.sparql

import com.signalcollect.triplerush.TripleRush
import org.scalatest.{Matchers, FlatSpec}
import scala.collection.JavaConversions.asScalaIterator

class Sparql11SyntaxTest extends FlatSpec with Matchers{
  
  it should "read manifest.ttl of syntax-fed and write a query against to get tests and their type in them" in {
    val tr = new TripleRush
    val manifestFile = "src/test/resources/sparql-1.1-w3c/syntax-fed/manifest.ttl"
    val graph = new TripleRushGraph(tr)
    implicit val model = graph.getModel
    tr.loadNtriples(manifestFile)
    tr.prepareExecution
    val query =
      """
        |		PREFIX mf:  <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>
        |		PREFIX qt:  <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>
        |   PREFIX dawgt: <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#>
        |   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |				SELECT ?TestURI ?Name ?Action ?Type
        |		WHERE { [] rdf:first ?TestURI.
        |		        ?TestURI a ?Type ;
        |		                 mf:name ?Name ;
        |		                 mf:action ?Action ;
        |		                 dawgt:approval dawgt:Approved .
        |		        FILTER(?Type IN (mf:PositiveSyntaxTest11, mf:NegativeSyntaxTest11, mf:PositiveUpdateSyntaxTest11, mf:NegativeUpdateSyntaxTest11))
        |		 }
      """.stripMargin
    val results = Sparql(query)
    results.size should be(3)
  }
  
}