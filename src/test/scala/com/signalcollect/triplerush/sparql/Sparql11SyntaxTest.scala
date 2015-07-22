package com.signalcollect.triplerush.sparql

import com.hp.hpl.jena.query.{QueryFactory, QueryParseException}
import com.signalcollect.triplerush.TripleRush
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConversions.asScalaIterator

class Sparql11SyntaxTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val tr = new TripleRush
  val graph = new TripleRushGraph(tr)
  implicit val model = graph.getModel
  var subManifests: List[String] = List.empty

  override def beforeAll:
  Unit = {
    //    val manifestFile = "src/test/resources/sparql-1.1-w3c/manifest-all.ttl"
    //    tr.loadNtriples(manifestFile)
    //    tr.awaitIdle
    //    tr.prepareExecution
    //    val subManifestQuery =
    //      """|PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    //        |PREFIX mf:     <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>
    //        |PREFIX qt:     <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>
    //        | SELECT ?item
    //        | WHERE {
    //        |  [] rdf:first ?item
    //        | }
    //        | """.stripMargin
    //    val subManifestsResultSet = Sparql(subManifestQuery)
    //    subManifests = subManifestsResultSet.map(f => f.get("item").toString).toList.distinct.filter(f => f.endsWith(".ttl"))
    subManifests = List("src/test/resources/sparql-1.1-w3c/bind/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/copy/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/delete-where/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/exists/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/move/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/service/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/syntax-update-2/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/protocol/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/construct/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/negation/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/syntax-query/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/add/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/delete-data/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/drop/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/update-silent/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/json-res/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/basic-update/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/delete-insert/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/entailment/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/property-path/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/syntax-update-1/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/service-description/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/aggregates/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/project-expression/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/bindings/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/csv-tsv-res/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/delete/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/functions/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/subquery/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/http-rdf-update/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/grouping/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/clear/manifest.ttl",
      "src/test/resources/sparql-1.1-w3c/syntax-fed/manifest.ttl")
  }

  def load(subManifests: List[String]):
  Unit = {
    subManifests.map {
      f =>
        tr.loadNtriples(f)
        tr.awaitIdle
    }
  }

  "TripleRush" should "pass syntax tests" in {
    println(subManifests.size)
    load(subManifests)
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
        |              FILTER(?Type IN (mf:PositiveSyntaxTest11, mf:NegativeSyntaxTest11, mf:PositiveUpdateSyntaxTest11, mf:NegativeUpdateSyntaxTest11))
        |		           }
      """.stripMargin
    val results = Sparql(query)

    case class TestDetails(uri: String, name: String, action: String, positive: Boolean)

    val testsToRun = results.map(test => {
      val uriOfTest = test.get("TestURI").toString
      val nameOfTest = test.get("Name").toString
      val action = test.get("Action").toString
      val typeOfTest = test.get("Type").toString
      val positiveTest = typeOfTest.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11") ||  
        typeOfTest.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11")
      TestDetails(uriOfTest, nameOfTest, action, positiveTest)
    }
    ).toList

    val expectedNumberOfPositiveTests = testsToRun.count(p => p.positive)
    val expectedNumberOfNegativeTests = testsToRun.count(p => !p.positive)
    
    var actualNumberOfPositivePassed = 0
    var actualNumberOfNegativePassed = 0
    testsToRun.map(test => {
      val query = scala.io.Source.fromFile(test.action.replace("file://", "")).mkString
      if (test.positive) {
        try {
            Sparql(query)
            actualNumberOfPositivePassed += 1
        } catch {
          case ex: Exception => println("failed to do anything with this query where as we should have: "+ query)
        } 
      }
      else {
        intercept[Exception] {
          actualNumberOfNegativePassed += 1
          Sparql(query)
        }
      }
    })
    
    println("Expected number of positives: " +expectedNumberOfPositiveTests)
    println("Expected number of negatives: " +expectedNumberOfNegativeTests)
    println("Actual number of positives: " +actualNumberOfPositivePassed)
    println("Actual number of negatives: " +actualNumberOfNegativePassed)
  }

}