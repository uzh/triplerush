package com.signalcollect.triplerush.sparql

import com.hp.hpl.jena.query.QueryParseException
import com.signalcollect.triplerush.TripleRush
import org.scalatest.{Matchers, FlatSpec}
import scala.collection.JavaConversions.asScalaIterator

class Sparql11SyntaxTest extends FlatSpec with Matchers{
  
  "TripleRush" should "pass tests in /syntax-fed" in {
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
        |	     	WHERE { [] rdf:first ?TestURI.
        |		           ?TestURI a ?Type ;
        |		                 mf:name ?Name ;
        |		                 mf:action ?Action ;
        |		                 dawgt:approval dawgt:Approved .
        |		           }
      """.stripMargin
    val results = Sparql(query)

    case class TestDetails(uri: String, name: String, action: String, positive: Boolean)
    
    val testsToRun = results.map(test => {
        val uriOfTest = test.get("TestURI").toString
        val nameOfTest = test.get("Name").toString
        val action = test.get("Action").toString
        val typeOfTest = test.get("Type").toString
        val positiveTest = typeOfTest.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11")
        TestDetails(uriOfTest, nameOfTest, action, positiveTest)
      }
    ).toList

    //TODO: Need to figure why java.lang.IllegalStateException is thrown for positive syntax test.
    testsToRun.map(test => {
      val query = scala.io.Source.fromFile(test.action.replace("file://","")).mkString
      if(test.positive) {
          Sparql(query)
      }
      else {
        intercept[QueryParseException] {
          Sparql(query)
        }
      }
    })
  }
}