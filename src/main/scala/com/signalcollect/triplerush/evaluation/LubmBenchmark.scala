/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
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

package com.signalcollect.triplerush.evaluation

import com.signalcollect.triplerush.evaluation.SparqlDsl._
import com.signalcollect.nodeprovisioning.torque._
import com.signalcollect.configuration._
import com.signalcollect._
import com.signalcollect.nodeprovisioning.local.LocalNodeProvisioner
import com.signalcollect.nodeprovisioning.Node
import com.typesafe.config.Config
import com.signalcollect.nodeprovisioning.torque.TorqueHost
import com.signalcollect.ExecutionConfiguration
import java.io.File
import com.signalcollect.triplerush.QueryEngine
import com.signalcollect.triplerush.PatternQuery
import scala.concurrent.Await
import com.signalcollect.triplerush.Bindings
import scala.concurrent.duration.FiniteDuration
import com.signalcollect.triplerush.Mapping
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.signalcollect.triplerush.QueryOptimizer
import scala.sys.process._
import scala.io.Source

/**
 * Runs a PageRank algorithm on a graph of a fixed size
 * for different numbers of worker threads.
 *
 * Evaluation is set to execute on a 'Kraken'-node.
 */
object LubmBenchmark extends App {
  val jvmHighThroughputGc = " -Xmx64000m" +
    " -Xms64000m" +
    " -Xmn8000m" +
    " -d64" +
    " -XX:+UnlockExperimentalVMOptions" +
    " -XX:+UseConcMarkSweepGC" +
    " -XX:+UseParNewGC" +
    " -XX:+CMSIncrementalPacing" +
    " -XX:+CMSIncrementalMode" +
    " -XX:ParallelGCThreads=20" +
    " -XX:ParallelCMSThreads=20" +
    " -XX:MaxInlineSize=1024"
  val jvmParameters = " -Xmx64000m" +
    " -Xms64000m"
  val assemblyPath = "./target/triplerush-assembly-1.0-SNAPSHOT.jar"
  val assemblyFile = new File(assemblyPath)
  val copyName = assemblyPath.replace("-SNAPSHOT", (Random.nextInt % 10000).toString)
  assemblyFile.renameTo(new File(copyName))
  val kraken = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    localJarPath = copyName, jvmParameters = jvmParameters, priority = TorquePriority.fast)
  val localHost = new LocalHost
  val googleDocs = new GoogleDocsResultHandler(args(0), args(1), "triplerush", "data")

  def getRevision: String = {
    try {
      val gitLogPath = ".git/logs/HEAD"
      val gitLog = new File(gitLogPath)
      val lines = Source.fromFile(gitLogPath).getLines
      val lastLine = lines.toList.last
      val revision = lastLine.split(" ")(1)
      revision
    } catch {
      case t: Throwable => "Unknown revision."
    }
  }

  /*********/
  def evalName = "LUBM benchmarking -- Index vertices collect and signal right upon signal delivery."
  //  def evalName = "Local debugging."
  val runs = 1
  var evaluation = new Evaluation(evaluationName = evalName, executionHost = kraken).addResultHandler(googleDocs)
  /*********/

  for (run <- 1 to runs) {
    for (queryId <- 1 to 7) {
      for (optimizer <- List(QueryOptimizer.None, QueryOptimizer.Greedy, QueryOptimizer.Clever)) {
        //for (tickets <- List(1000, 10000, 100000, 1000000)) {
        //evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, queryId, true, tickets))
        //        evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, queryId, false, tickets))
        //      }
        evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, queryId, false, Long.MaxValue, optimizer, 160, getRevision))
      }
    }
  }
  evaluation.execute

  def lubmBenchmarkRun(description: String, queryId: Int, sampling: Boolean, tickets: Long, optimizer: QueryOptimizer.Value, loadNumber: Int, revision: String)(): List[Map[String, String]] = {
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"

    Mapping.setAbbreviations(Map(
      ub -> "ub:",
      rdf -> "rdf:",
      "http://www" -> "www",
      "Department" -> "D",
      "University" -> "U",
      ".edu/" -> "/",
      "FullProfessor" -> "FP",
      "AssociateProfessor" -> "ACP",
      "AssistantProfessor" -> "ASP",
      "Lecturer" -> "L",
      "Undergraduate" -> "UG",
      "Student" -> "S",
      "Graduate" -> "G",
      "ResearchGroup" -> "RG",
      "Publication" -> "P",
      "Course" -> "C",
      "xxx-xxx-xxxx" -> "?",
      "telephone" -> "tel",
      "emailAddress" -> "email",
      "publicationAuthor" -> "author",
      "undergraduateDegreeFrom" -> "UGDF",
      "subOrganizationOf" -> "subOrg"))

    /**
     * Queries from: http://www.cs.rpi.edu/~zaki/PaperDir/WWW10.pdf
     * Result sizes from: http://research.microsoft.com/pubs/183717/Trinity.RDF.pdf
     *            L1   L2       L3 L4 L5 L6  L7
     * LUBM-160   397  173040   0  10 10 125 7125
     * LUBM-10240 2502 11016920 0  10 10 125 450721
     *
     * Times Trinity: 281 132 110  5    4 9 630
     * Time TripleR: 3815 222 3126 2    1 2 603
     */
    def fullQueries: List[PatternQuery] = List(
      SELECT ? "X" ? "Y" ? "Z" WHERE (
        | - "X" - s"$rdf#type" - s"$ub#GraduateStudent",
        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
        | - "X" - s"$ub#memberOf" - "Z",
        | - "Z" - s"$rdf#type" - s"$ub#Department",
        | - "Z" - s"$ub#subOrganizationOf" - "Y",
        | - "Y" - s"$rdf#type" - s"$ub#University"),
      SELECT ? "X" ? "Y" WHERE (
        | - "X" - s"$rdf#type" - s"$ub#Course",
        | - "X" - s"$ub#name" - "Y"),
      SELECT ? "X" ? "Y" ? "Z" WHERE (
        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent",
        | - "X" - s"$ub#memberOf" - "Z",
        | - "Z" - s"$ub#subOrganizationOf" - "Y",
        | - "Z" - s"$rdf#type" - s"$ub#Department",
        | - "Y" - s"$rdf#type" - s"$ub#University"),
      SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
        | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
        | - "X" - s"$rdf#type" - s"$ub#FullProfessor",
        | - "X" - s"$ub#name" - "Y1",
        | - "X" - s"$ub#emailAddress" - "Y2",
        | - "X" - s"$ub#telephone" - "Y3"),
      SELECT ? "X" WHERE (
        | - "X" - s"$ub#subOrganizationOf" - "http://www.Department0.University0.edu",
        | - "X" - s"$rdf#type" - s"$ub#ResearchGroup"),
      SELECT ? "X" ? "Y" WHERE (
        | - "Y" - s"$ub#subOrganizationOf" - "http://www.University0.edu",
        | - "Y" - s"$rdf#type" - s"$ub#Department",
        | - "X" - s"$ub#worksFor" - "Y",
        | - "X" - s"$rdf#type" - s"$ub#FullProfessor"),
      SELECT ? "X" ? "Y" ? "Z" WHERE (
        | - "Y" - s"$rdf#type" - s"$ub#FullProfessor",
        | - "Y" - s"$ub#teacherOf" - "Z",
        | - "Z" - s"$rdf#type" - s"$ub#Course",
        | - "X" - s"$ub#advisor" - "Y",
        | - "X" - s"$ub#takesCourse" - "Z",
        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent"))

    def boundedQueries(sampleSize: Long): List[PatternQuery] = List(
      BOUNDED(sampleSize) ? "X" ? "Y" ? "Z" WHERE (
        | - "X" - s"$rdf#type" - s"$ub#GraduateStudent",
        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
        | - "X" - s"$ub#memberOf" - "Z",
        | - "Z" - s"$rdf#type" - s"$ub#Department",
        | - "Z" - s"$ub#subOrganizationOf" - "Y",
        | - "Y" - s"$rdf#type" - s"$ub#University"),
      BOUNDED(sampleSize) ? "X" ? "Y" WHERE (
        | - "X" - s"$rdf#type" - s"$ub#Course",
        | - "X" - s"$ub#name" - "Y"),
      BOUNDED(sampleSize) ? "X" ? "Y" ? "Z" WHERE (
        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent",
        | - "X" - s"$ub#memberOf" - "Z",
        | - "Z" - s"$ub#subOrganizationOf" - "Y",
        | - "Z" - s"$rdf#type" - s"$ub#Department",
        | - "Y" - s"$rdf#type" - s"$ub#University"),
      BOUNDED(sampleSize) ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
        | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
        | - "X" - s"$rdf#type" - s"$ub#FullProfessor",
        | - "X" - s"$ub#name" - "Y1",
        | - "X" - s"$ub#emailAddress" - "Y2",
        | - "X" - s"$ub#telephone" - "Y3"),
      BOUNDED(sampleSize) ? "X" WHERE (
        | - "X" - s"$ub#subOrganizationOf" - "http://www.Department0.University0.edu",
        | - "X" - s"$rdf#type" - s"$ub#ResearchGroup"),
      BOUNDED(sampleSize) ? "X" ? "Y" WHERE (
        | - "Y" - s"$ub#subOrganizationOf" - "http://www.University0.edu",
        | - "Y" - s"$rdf#type" - s"$ub#Department",
        | - "X" - s"$ub#worksFor" - "Y",
        | - "X" - s"$rdf#type" - s"$ub#FullProfessor"),
      BOUNDED(sampleSize) ? "X" ? "Y" ? "Z" WHERE (
        | - "Y" - s"$rdf#type" - s"$ub#FullProfessor",
        | - "Y" - s"$ub#teacherOf" - "Z",
        | - "Z" - s"$rdf#type" - s"$ub#Course",
        | - "X" - s"$ub#advisor" - "Y",
        | - "X" - s"$ub#takesCourse" - "Z",
        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent"))

    def samplingQueries(sampleSize: Long): List[PatternQuery] = List(
      SAMPLE(sampleSize) ? "X" ? "Y" ? "Z" WHERE (
        | - "X" - s"$rdf#type" - s"$ub#GraduateStudent",
        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
        | - "X" - s"$ub#memberOf" - "Z",
        | - "Z" - s"$rdf#type" - s"$ub#Department",
        | - "Z" - s"$ub#subOrganizationOf" - "Y",
        | - "Y" - s"$rdf#type" - s"$ub#University"),
      SAMPLE(sampleSize) ? "X" ? "Y" WHERE (
        | - "X" - s"$rdf#type" - s"$ub#Course",
        | - "X" - s"$ub#name" - "Y"),
      SAMPLE(sampleSize) ? "X" ? "Y" ? "Z" WHERE (
        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent",
        | - "X" - s"$ub#memberOf" - "Z",
        | - "Z" - s"$ub#subOrganizationOf" - "Y",
        | - "Z" - s"$rdf#type" - s"$ub#Department",
        | - "Y" - s"$rdf#type" - s"$ub#University"),
      SAMPLE(sampleSize) ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
        | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
        | - "X" - s"$rdf#type" - s"$ub#FullProfessor",
        | - "X" - s"$ub#name" - "Y1",
        | - "X" - s"$ub#emailAddress" - "Y2",
        | - "X" - s"$ub#telephone" - "Y3"),
      SAMPLE(sampleSize) ? "X" WHERE (
        | - "X" - s"$ub#subOrganizationOf" - "http://www.Department0.University0.edu",
        | - "X" - s"$rdf#type" - s"$ub#ResearchGroup"),
      SAMPLE(sampleSize) ? "X" ? "Y" WHERE (
        | - "Y" - s"$ub#subOrganizationOf" - "http://www.University0.edu",
        | - "Y" - s"$rdf#type" - s"$ub#Department",
        | - "X" - s"$ub#worksFor" - "Y",
        | - "X" - s"$rdf#type" - s"$ub#FullProfessor"),
      SAMPLE(sampleSize) ? "X" ? "Y" ? "Z" WHERE (
        | - "Y" - s"$rdf#type" - s"$ub#FullProfessor",
        | - "Y" - s"$ub#teacherOf" - "Z",
        | - "Z" - s"$rdf#type" - s"$ub#Course",
        | - "X" - s"$ub#advisor" - "Y",
        | - "X" - s"$ub#takesCourse" - "Z",
        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent"))

    val queries = {
      if (sampling) {
        samplingQueries(tickets)
      } else {
        if (tickets == Long.MaxValue) {
          fullQueries
        } else {
          boundedQueries(tickets)
        }
      }
    }

    var baseResults = Map[String, String]()
    val qe = new QueryEngine()

    def loadLubm(numberOfUniversities: Int = 160) {
      val lubm160FolderName = "lubm160"
      for (university <- (0 until numberOfUniversities)) {
        for (subfile <- (0 to 99)) {
          val potentialFileName = s"$lubm160FolderName/University${university}_$subfile.nt"
          val potentialFile = new File(potentialFileName)
          if (potentialFile.exists) {
            qe.load(potentialFileName)
          }
        }
      }
      qe.prepareQueryExecution
    }

    /**
     * Returns the time in milliseconds it takes to execute the code in 'codeBlock'.
     */
    def measureTime(codeBlock: => Unit): Long = {
      val startTime = System.currentTimeMillis
      codeBlock
      val finishTime = System.currentTimeMillis
      finishTime - startTime
    }

    def roundToMillisecondFraction(nanoseconds: Long): Double = {
      ((nanoseconds / 100000.0).round) / 10.0
    }

    def executeOnQueryEngine(q: PatternQuery): (List[PatternQuery], Map[String, Any]) = {
      val resultFuture = qe.executeQuery(q, optimizer)
      try {
        Await.result(resultFuture, new FiniteDuration(100, TimeUnit.SECONDS)) // TODO handle exception
      } catch {
        case t: Throwable =>
          println(s"Query $q timed out!")
          (List(), Map("exception" -> t).withDefaultValue(""))
      }
    }

    /**
     * Go to JVM JIT steady state by executing the query 100 times.
     */
    def jitSteadyState {
      for (i <- 1 to 100) {
        val queryIndex = queryId - 1
        val query = fullQueries(queryIndex)
        executeOnQueryEngine(query)
        qe.awaitIdle
      }
    }

    def cleanGarbage {
      for (i <- 1 to 10) {
        System.gc
        Thread.sleep(100)
      }
      Thread.sleep(10000)
    }

    var finalResults = List[Map[String, String]]()
    def runEvaluation(queryId: Int) {
      var runResult = baseResults
      val queryIndex = queryId - 1
      val query = queries(queryIndex)
      val startTime = System.nanoTime
      val queryResult = executeOnQueryEngine(query)
      val finishTime = System.nanoTime
      val executionTime = roundToMillisecondFraction(finishTime - startTime)
      val timeToFirstResult = roundToMillisecondFraction(queryResult._2("firstResultNanoTime").asInstanceOf[Long] - startTime)
      val optimizingTime = roundToMillisecondFraction(queryResult._2("optimizingDuration").asInstanceOf[Long])
      runResult += s"revision" -> revision
      runResult += s"queryId" -> queryId.toString
      runResult += s"optimizer" -> optimizer.toString
      runResult += s"exception" -> queryResult._2("exception").toString
      runResult += s"results" -> queryResult._1.length.toString
      runResult += s"samplingQuery" -> query.isSamplingQuery.toString
      runResult += s"tickets" -> query.tickets.toString
      runResult += s"executionTime" -> executionTime.toString
      runResult += s"timeUntilFirstResult" -> timeToFirstResult.toString
      runResult += s"optimizingTime" -> optimizingTime.toString
      runResult += s"totalMemory" -> bytesToGigabytes(Runtime.getRuntime.totalMemory).toString
      runResult += s"freeMemory" -> bytesToGigabytes(Runtime.getRuntime.freeMemory).toString
      runResult += s"usedMemory" -> bytesToGigabytes(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory).toString
      runResult += s"executionHostname" -> java.net.InetAddress.getLocalHost.getHostName
      runResult += s"loadNumber" -> loadNumber.toString
      finalResults = runResult :: finalResults
    }

    def bytesToGigabytes(bytes: Long): Double = ((bytes / 1073741824.0) * 10.0).round / 10.0

    baseResults += "evaluationDescription" -> description
    val loadingTime = measureTime {
      loadLubm(loadNumber)
      qe.awaitIdle
    }
    baseResults += "loadingTime" -> loadingTime.toString

    jitSteadyState
    cleanGarbage
    runEvaluation(queryId)
    qe.awaitIdle
    qe.shutdown
    finalResults
  }

}