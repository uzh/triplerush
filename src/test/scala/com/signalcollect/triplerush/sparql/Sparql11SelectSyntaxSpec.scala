package com.signalcollect.triplerush.sparql

import com.signalcollect.triplerush.TripleRush
import com.signalcollect.util.TestAnnouncements
import org.apache.jena.query.QueryFactory
import org.scalatest.prop.Checkers
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.collection.JavaConversions.asScalaIterator

/**
 * Uses w3c test files to run SELECT syntax tests against Sparql 1.1 spec*
 */
//TODO: Work with "sesame-sparql-testsuite" to remove test files from resources directory.
class Sparql11SelectSyntaxSpec extends FlatSpec with Matchers with TestAnnouncements {

  val tr = new TripleRush
  val graph = new TripleRushGraph(tr)
  implicit val model = graph.getModel

  "TripleRush" should "pass SELECT Sparql-1.1 syntax tests" in {
    val manifestFile = "src/test/resources/sparql-1.1-w3c/manifest-all.ttl"
    //Load main manifest.
    tr.load(manifestFile)
    tr.awaitIdle
    tr.prepareExecution
    //Retrieve sub-manifests
    val subManifestQuery =
      """|PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX mf:     <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>
         |PREFIX qt:     <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>
         |SELECT DISTINCT ?item
         | WHERE {
         |  [] rdf:first ?item
         | }
         | """.stripMargin
    val subManifestsResultSet = Sparql(subManifestQuery)
    val subManifests = subManifestsResultSet.map(f => f.get("item").toString).toList.filter(f => f.endsWith(".ttl"))
    //Load sub-manifests.
    subManifests.map {
      subManifest =>
        tr.load(subManifest)
        tr.awaitIdle
    }
    tr.prepareExecution
    //Retrieve location of query to run and type(whether it could parse or not).
    val query =
      """
        |		PREFIX mf:  <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>
        |		PREFIX qt:  <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>
        |   PREFIX dawgt: <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#>
        |   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |				SELECT ?queryToRun ?type
        |	     	WHERE { [] rdf:first ?testURI.
        |                    ?testURI a ?type ;
        |		                 mf:action ?queryToRun ;
        |		                 dawgt:approval dawgt:Approved .
        |       FILTER(?type IN (mf:PositiveSyntaxTest11, mf:NegativeSyntaxTest11, mf:PositiveUpdateSyntaxTest11, mf:NegativeUpdateSyntaxTest11))
        |		           }
      """.stripMargin
    val results = Sparql(query)

    case class Test(queryToRun: String, positive: Boolean)

    val testsToRun = results.map(test => {
      val queryToRun = test.get("queryToRun").toString
      val typeOfTest = test.get("type").toString
      val positiveTest = typeOfTest.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11") ||
        typeOfTest.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11")
      Test(queryToRun, positiveTest)
    }).toList

    var expectedNumOfPositiveTests = 0
    val expectedNumOfNegativeTests = testsToRun.count(p => !p.positive)
    var actualNumOfPositivePassed = 0
    var actualNumOfNegativePassed = 0
    testsToRun.map(test => {
      val query = scala.io.Source.fromFile(test.queryToRun.replace("file://", "")).mkString
      if (test.positive) {
        try {
          val queryFactoryQuery = QueryFactory.create(query)
          if (queryFactoryQuery.isSelectType && !query.contains("SERVICE")) {
            expectedNumOfPositiveTests += 1
            Sparql(query)
            actualNumOfPositivePassed += 1
          }
        } catch {
          case parseException: org.apache.jena.query.QueryParseException => // This is expected because QueryFactory.create works only
            // on QUERY and not on UPDATE, LOAD.
          case illegalStateException: java.lang.IllegalStateException => //This one is expected as "SERVICE" isn't working or
          //even Jena probably doesn't work for this query.
        }
      }
      else {
        intercept[Exception] {
          actualNumOfNegativePassed += 1
          Sparql(query)
        }
      }
    })
    tr.shutdown()
    actualNumOfPositivePassed should be(expectedNumOfPositiveTests)
    actualNumOfNegativePassed should be(expectedNumOfNegativeTests)
  }

}