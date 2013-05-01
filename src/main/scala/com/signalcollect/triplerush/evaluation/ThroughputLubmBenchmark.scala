///*
// *  @author Philip Stutz
// *  @author Mihaela Verman
// *  
// *  Copyright 2013 University of Zurich
// *      
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *  
// *         http://www.apache.org/licenses/LICENSE-2.0
// *  
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *  
// */
//
//package com.signalcollect.triplerush.evaluation
//
//import java.io.File
//import scala.concurrent.Await
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration.DurationInt
//import scala.concurrent.future
//import com.signalcollect.nodeprovisioning.torque.LocalHost
//import com.signalcollect.nodeprovisioning.torque.TorqueHost
//import com.signalcollect.nodeprovisioning.torque.TorqueJobSubmitter
//import com.signalcollect.nodeprovisioning.torque.TorquePriority
//import com.signalcollect.triplerush.Mapping
//import com.signalcollect.triplerush.PatternQuery
//import com.signalcollect.triplerush.QueryEngine
//import com.signalcollect.triplerush.evaluation.SparqlDsl.SELECT
//import com.signalcollect.triplerush.evaluation.SparqlDsl.dsl2Query
//import com.signalcollect.triplerush.evaluation.SparqlDsl.{| => |}
//import com.signalcollect.triplerush.Mapping
//
///**
// * Runs a PageRank algorithm on a graph of a fixed size
// * for different numbers of worker threads.
// *
// * Evaluation is set to execute on a 'Kraken'-node.
// */
//object ThroughputLubmBenchmark extends App {
//  val jvmParameters = " -Xmx64000m" +
//    " -Xms64000m" +
//    " -Xmn8000m" +
//    " -d64" +
//    " -XX:+UnlockExperimentalVMOptions" +
//    " -XX:+UseConcMarkSweepGC" +
//    " -XX:+UseParNewGC" +
//    " -XX:+CMSIncrementalPacing" +
//    " -XX:+CMSIncrementalMode" +
//    " -XX:ParallelGCThreads=20" +
//    " -XX:ParallelCMSThreads=20" +
//    " -XX:MaxInlineSize=1024"
//  //  val jvmParameters = " -Xmx64000m" +
//  //    " -Xms64000m"
//  val kraken = new TorqueHost(
//    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
//    localJarPath = "./target/triplerush-assembly-1.0-SNAPSHOT.jar", jvmParameters = jvmParameters, priority = TorquePriority.fast)
//  val localHost = new LocalHost
//  val googleDocs = new GoogleDocsResultHandler(args(0), args(1), "triplerush-throughput", "data")
//
//  /*********/
//  def evalName = "Throughput LUBM benchmarking - Futures - Ela"
//  //  def evalName = "Local debugging."
//  val runs = 1
//  var evaluation = new Evaluation(evaluationName = evalName, executionHost = kraken).addResultHandler(googleDocs)
//  /*********/
//
//  for (run <- 1 to runs) {
//    for (queryId <- 1 to 1) {
//      //for (tickets <- List(1000, 10000, 100000, 1000000)) {
//      //evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, queryId, true, tickets))
//      //        evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, queryId, false, tickets))
//      //      }
//      evaluation = evaluation.addEvaluationRun(throughputLubmBenchmarkRun(evalName, queryId, false, Long.MaxValue))
//    }
//  }
//  evaluation.execute
//
//  def throughputLubmBenchmarkRun(description: String, queryId: Int, sampling: Boolean, tickets: Long)(): List[Map[String, String]] = {
//    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
//    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
//
//    Mapping.setAbbreviations(Map(
//      ub -> "ub:",
//      rdf -> "rdf:",
//      "http://www" -> "www",
//      "Department" -> "D",
//      "University" -> "U",
//      ".edu/" -> "/",
//      "FullProfessor" -> "FP",
//      "AssociateProfessor" -> "ACP",
//      "AssistantProfessor" -> "ASP",
//      "Lecturer" -> "L",
//      "Undergraduate" -> "UG",
//      "Student" -> "S",
//      "Graduate" -> "G",
//      "ResearchGroup" -> "RG",
//      "Publication" -> "P",
//      "Course" -> "C",
//      "xxx-xxx-xxxx" -> "?",
//      "telephone" -> "tel",
//      "emailAddress" -> "email",
//      "publicationAuthor" -> "author",
//      "undergraduateDegreeFrom" -> "UGDF",
//      "subOrganizationOf" -> "subOrg"))
//
//    /**
//     * Queries from: http://www.cs.rpi.edu/~zaki/PaperDir/WWW10.pdf
//     * Result sizes from: http://research.microsoft.com/pubs/183717/Trinity.RDF.pdf
//     *            L1   L2       L3 L4 L5 L6  L7
//     * LUBM-160   397  173040   0  10 10 125 7125
//     * LUBM-10240 2502 11016920 0  10 10 125 450721
//     *
//     * Times Trinity: 281 132 110  5    4 9 630
//     * Time TripleR: 3815 222 3126 2    1 2 603
//     */
//
//    def query1: PatternQuery = {
//      SELECT ? "X" ? "Y" ? "Z" WHERE (
//        | - "X" - s"$rdf#type" - s"$ub#GraduateStudent",
//        | - "X" - s"$ub#undergraduateDegreeFrom" - "Y",
//        | - "X" - s"$ub#memberOf" - "Z",
//        | - "Z" - s"$rdf#type" - s"$ub#Department",
//        | - "Z" - s"$ub#subOrganizationOf" - "Y",
//        | - "Y" - s"$rdf#type" - s"$ub#University")
//    }
//
//    def query6(uniNumberString: String): PatternQuery = {
//      SELECT ? "X" ? "Y" WHERE (
//        | - "Y" - s"$ub#subOrganizationOf" - s"http://www.University$uniNumberString.edu",
//        | - "Y" - s"$rdf#type" - s"$ub#Department",
//        | - "X" - s"$ub#worksFor" - "Y",
//        | - "X" - s"$rdf#type" - s"$ub#FullProfessor")
//    }
//
//    def query7: PatternQuery = {
//      SELECT ? "X" ? "Y" ? "Z" WHERE (
//        | - "Y" - s"$rdf#type" - s"$ub#FullProfessor",
//        | - "Y" - s"$ub#teacherOf" - "Z",
//        | - "Z" - s"$rdf#type" - s"$ub#Course",
//        | - "X" - s"$ub#advisor" - "Y",
//        | - "X" - s"$ub#takesCourse" - "Z",
//        | - "X" - s"$rdf#type" - s"$ub#UndergraduateStudent")
//    }
//
//    def fullThroughputQueries6: List[PatternQuery] = (0 to 159).map(uniNumber => query6(uniNumber.toString())).toList
//
//    val warmUpQueries = {
//      fullThroughputQueries6
//    }
//
//    val evaluatedQueries = {
//      fullThroughputQueries6
//    }
//
//    var baseResults = Map[String, String]()
//    val qe = new QueryEngine()
//
//    def loadLubm160 {
//      val lubm160FolderName = "lubm160"
//      val folder = new File(lubm160FolderName)
//      for (file <- folder.listFiles) {
//        if (file.getName.startsWith("University") && file.getName.endsWith(".nt")) {
//          qe.loadNtriples(file.getAbsolutePath)
//        }
//      }
//    }
//
//    /**
//     * Returns the time in milliseconds it takes to execute the code in 'codeBlock'.
//     */
//    def measureTime(codeBlock: => Unit): Long = {
//      val startTime = System.currentTimeMillis
//      codeBlock
//      val finishTime = System.currentTimeMillis
//      finishTime - startTime
//    }
//
//    def roundToMillisecondFraction(nanoseconds: Long): Double = {
//      ((nanoseconds / 100000.0).round) / 10.0
//    }
//
//    def executeMultipleOnQueryEngine(qList: List[PatternQuery]): List[(List[PatternQuery], Map[String, Any])] = {
// //   		qList.par.map(q => executeOnQueryEngine(q)).toList  
//      
//      val resultFutures = qList.map(q => {
//        future {
//          executeOnQueryEngine(q) 
//        }
//      })
//      resultFutures.map(Await.result(_, 1000 seconds))
//      
//    }
//
//    def executeOnQueryEngine(q: PatternQuery): (List[PatternQuery], Map[String, Any]) = {
//      val resultFuture = qe.executeQuery(q)
//      val result = Await.result(resultFuture, 1000 seconds)
//      result
//    }
//
//    /**
//     * Go to JVM JIT steady state by executing the full versions of other queries 10 times.
//     */
//    def jitSteadyState {
//      for (i <- 1 to 1) {
//        val queryList = warmUpQueries //TODO
//        executeMultipleOnQueryEngine(queryList)
//        qe.awaitIdle
//      }
//    }
//
//    def cleanGarbage {
//      for (i <- 1 to 10) {
//        System.gc
//        Thread.sleep(100)
//      }
//      Thread.sleep(10000)
//    }
//
//    var finalResults = List[Map[String, String]]()
//    def runEvaluation(queryId: Int) {
//      var runResult = baseResults
//
//      val startTime = System.nanoTime
//
//      val queryList = evaluatedQueries //TODO are they executed in parallel?
//      val queryResult = executeMultipleOnQueryEngine(queryList)
//
//      val finishTime = System.nanoTime
//      val executionTime = roundToMillisecondFraction(finishTime - startTime)
//      //val timeToFirstResult = roundToMillisecondFraction(queryResult._2("firstResultNanoTime").asInstanceOf[Long] - startTime)
//      runResult += s"queryId" -> queryId.toString
//      //runResult += s"results" -> queryResult._1.length.toString
//      //runResult += s"samplingQuery" -> query.isSamplingQuery.toString
//      //runResult += s"tickets" -> query.tickets.toString
//      runResult += s"executionTime" -> executionTime.toString
//      //runResult += s"timeUntilFirstResult" -> timeToFirstResult.toString
//      finalResults = runResult :: finalResults
//    }
//
//    baseResults += "evaluationDescription" -> description
//    val loadingTime = measureTime {
//      loadLubm160
//      qe.awaitIdle
//    }
//    baseResults += "loadingTime" -> loadingTime.toString
//
//    jitSteadyState
//    cleanGarbage
//    runEvaluation(queryId)
//    qe.awaitIdle
//    qe.shutdown
//    finalResults
//  }
//
//}