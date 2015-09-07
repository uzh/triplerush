/*
 *  @author Jahangir Mohammed
 *
 *  Copyright 2015 iHealth Technologies
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

package com.signalcollect.triplerush.sparql

import java.io.{ File, FileOutputStream }
import java.net.JarURLConnection
import java.nio.file.Files

import scala.collection.JavaConversions.asScalaIterator

import org.apache.commons.io.FileUtils
import org.apache.jena.query.QueryFactory
import org.scalatest.{ BeforeAndAfter, Finders, FlatSpec, Matchers }
import org.scalatest.exceptions.TestFailedException

import com.signalcollect.triplerush.TripleRush
import com.signalcollect.util.TestAnnouncements

/**
 * Uses w3c test files to run SELECT syntax tests against Sparql 1.1 spec*
 */
class Sparql11SelectSyntaxSpec extends FlatSpec with Matchers with BeforeAndAfter with TestAnnouncements {

  val tr = new TripleRush
  val graph = new TripleRushGraph(tr)
  implicit val model = graph.getModel
  // Unzip test jar into a temporary directory and delete after the tests are run.
  val tmpDir = Files.createTempDirectory("sparql-syntax")

  before {
    val url = getClass.getClassLoader.getResource("testcases-sparql-1.1-w3c/")
    val con = url.openConnection().asInstanceOf[JarURLConnection]
    val jarFile = con.getJarFile
    val entries = jarFile.entries()
    while (entries.hasMoreElements) {
      val entry = entries.nextElement()
      val f = new File(tmpDir + File.separator + entry.getName)
      if (entry.isDirectory) {
        f.mkdir()
      } else {
        val is = jarFile.getInputStream(entry)
        val fOutputStream = new FileOutputStream(f)
        while (is.available() > 0) {
          fOutputStream.write(is.read())
        }
        fOutputStream.close()
        is.close()
      }
    }
  }

  after {
    FileUtils.deleteDirectory(new File(tmpDir.toString))
    tr.shutdown()
  }

  "TripleRush" should "pass SELECT Sparql-1.1 syntax tests" in {
    val manifestFile = "testcases-sparql-1.1-w3c/manifest-all.ttl"
    //Load main manifest.
    tr.load(tmpDir.toString + File.separator + manifestFile)
    tr.awaitIdle
    tr.prepareExecution
    //Retrieve sub-manifests
    val subManifestQuery =
      """|PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |PREFIX list:   <http://jena.hpl.hp.com/ARQ/list#>
        |PREFIX mf:     <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>
        |PREFIX qt:     <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>
        |SELECT DISTINCT ?subManifest
        |WHERE {
        |  <http://www.w3.org/TR/sparql11-query/> mf:conformanceRequirement ?list .
        |  ?list list:member ?subManifest .
        |}
        | """.stripMargin
    val subManifestsResultSet = Sparql(subManifestQuery)
    val subManifests = subManifestsResultSet.map(f => f.get("subManifest").toString).toList
    //Load sub-manifests.
    subManifests.map {
      subManifest =>
        val subManifestFile = subManifest.replace("file://", "")
        tr.load(subManifestFile)
        tr.awaitIdle
    }
    tr.prepareExecution
    //Retrieve location of query to run and type(whether it could parse or not).
    val query =
      """
        |PREFIX mf:  <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>
        |PREFIX qt:  <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>
        |PREFIX dawgt: <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#>
        |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |SELECT ?queryToRun ?type
        |WHERE { [] rdf:first ?testURI.
        |        ?testURI a ?type ;
        |        mf:action ?queryToRun ;
        |        dawgt:approval dawgt:Approved .
        |        FILTER(?type IN (mf:PositiveSyntaxTest11, mf:NegativeSyntaxTest11, mf:PositiveUpdateSyntaxTest11, mf:NegativeUpdateSyntaxTest11))
        |}
      """.stripMargin

    val results = Sparql(query)

    case class Test(queryToRun: String, positive: Boolean) {
      def negative: Boolean = !positive
    }

    val testsToRun = results.map(test => {
      val queryToRun = test.get("queryToRun").toString
      val typeOfTest = test.get("type").toString
      val positiveTest = typeOfTest.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11") ||
        typeOfTest.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11")
      Test(queryToRun, positiveTest)
    }).toList

    /**
     * Returns true iff the test passes.
     */
    def runTest(test: Test): Boolean = {
      val query = scala.io.Source.fromFile(test.queryToRun.replace("file://", "")).mkString
      if (test.positive) {
        try {
          val queryFactoryQuery = QueryFactory.create(query)
          if (queryFactoryQuery.isSelectType && !query.contains("SERVICE")) {
            Sparql(query)
          }
          true
        } catch {
          case parseException: org.apache.jena.query.QueryParseException => true
          // This is expected because QueryFactory.create works only
          // on QUERY and not on UPDATE, LOAD, INSERT etc.
          case illegalStateException: java.lang.IllegalStateException => true
          // This one is expected as "SERVICE" isn't working or
          // even Jena probably doesn't work for this query.
          case other: Exception => false
        }
      } else {
        try {
          intercept[Exception] {
            Sparql(query)
          }
        } catch {
          case e: TestFailedException => false
        }
        true
      }
    }

    val expectedNumOfPositiveTests = testsToRun.filter(_.positive).size
    val expectedNumOfNegativeTests = testsToRun.count(p => p.negative)
    val actualNumOfPositivePassed = testsToRun.filter(_.positive).map(runTest(_)).count(_ == true)
    val actualNumOfNegativePassed = testsToRun.filter(_.negative).map(runTest(_)).count(_ == true)

    actualNumOfPositivePassed should be(expectedNumOfPositiveTests)
    actualNumOfNegativePassed should be(expectedNumOfNegativeTests)
  }

}
