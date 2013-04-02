package com.signalcollect.triplerush.benchmarking
import com.signalcollect.triplerush.SparqlDsl._
import com.signalcollect.nodeprovisioning.torque._
import com.signalcollect.configuration._
import com.signalcollect._
import com.signalcollect.nodeprovisioning.local.LocalNodeProvisioner
import com.signalcollect.nodeprovisioning.Node
import com.signalcollect.nodeprovisioning.local.LocalNode
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

/**
 * Runs a PageRank algorithm on a graph of a fixed size
 * for different numbers of worker threads.
 *
 * Evaluation is set to execute on a 'Kraken'-node.
 */
object LubmBenchmark extends App {
  val jvmParameters = " -Xmx64000m" +
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
  val kraken = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    localJarPath = "./target/triplerush-assembly-1.0-SNAPSHOT.jar", jvmParameters = jvmParameters, priority = TorquePriority.fast)
  val localHost = new LocalHost
  val googleDocs = new GoogleDocsResultHandler(args(0), args(1), "triplerush", "data")

  /*********/
  def evalName = "LUBM benchmarking with both full and sampling queries, measuring the time until first responses as well."
//  def evalName = "Local debugging."
  val runs = 10
  var evaluation = new Evaluation(evaluationName = evalName, executionHost = kraken).addResultHandler(googleDocs)
  /*********/

  for (run <- 1 to runs) {
    for (sampleSize <- List(10000, 100000, 1000000, 10000000)) {
      evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, Some(sampleSize)))
    }
  }
  for (run <- 1 to runs) {
    evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, None))
  }
  evaluation.execute

  def lubmBenchmarkRun(description: String, sampling: Option[Int])(): Map[String, String] = {
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

    def samplingQueries(sampleSize: Int): List[PatternQuery] = List(
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
      if (sampling.isDefined) {
        samplingQueries(sampling.get)
      } else {
        fullQueries
      }
    }

    var results = Map[String, String]()
    results += "evaluationDescription" -> evalName
    val toRenameFolder = "lubm160"
    val folder = new File(toRenameFolder)
    val qe = new QueryEngine()
    def loadLubm160 {
      for (file <- folder.listFiles) {
        if (file.getName.startsWith("University") && file.getName.endsWith(".nt")) {
          qe.load(file.getAbsolutePath)
        }
      }
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
      val resultFuture = qe.executeQuery(q)
      val result = Await.result(resultFuture, new FiniteDuration(1000, TimeUnit.SECONDS))
      result
    }

    val loadingTime = measureTime {
      loadLubm160
      qe.awaitIdle
    }
    results += "loadingTime" -> loadingTime.toString
    results += "evaluationDescription" -> description

    for (queryId <- 1 to 7) {
      val queryIndex = queryId - 1
      val query = queries(queryIndex)
      val startTime = System.nanoTime
      val queryResult = executeOnQueryEngine(query)
      val finishTime = System.nanoTime
      val executionTime = roundToMillisecondFraction(finishTime - startTime)
      val timeToFirstResult = roundToMillisecondFraction(queryResult._2("firstResultNanoTime").asInstanceOf[Long] - startTime)
      results += s"resultsQuery$queryId" -> queryResult._1.length.toString
      results += s"samplingQuery" -> query.isSamplingQuery.toString
      results += s"sampleSize" -> query.tickets.toString
      results += s"executionTimeQuery$queryId" -> executionTime.toString
      results += s"timeUntilFirstResult$queryId" -> timeToFirstResult.toString
    }
    qe.awaitIdle
    qe.shutdown
    results
  }

}