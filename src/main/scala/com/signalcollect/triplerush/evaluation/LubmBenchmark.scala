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
//import java.util.Date
//import java.util.concurrent.TimeUnit
//import scala.concurrent.Await
//import scala.concurrent.duration.FiniteDuration
//import scala.concurrent.duration._
//import scala.io.Source
//import scala.util.Random
//import com.signalcollect.GraphBuilder
//import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
//import com.signalcollect.nodeprovisioning.torque.LocalHost
//import com.signalcollect.nodeprovisioning.torque.TorqueHost
//import com.signalcollect.nodeprovisioning.torque.TorqueJobSubmitter
//import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
//import com.signalcollect.nodeprovisioning.torque.TorquePriority
//import com.signalcollect.triplerush.Mapping
//import com.signalcollect.triplerush.QueryParticle
//import com.signalcollect.triplerush.QueryEngine
//import com.signalcollect.triplerush.vertices.QueryOptimizer
//import com.signalcollect.triplerush.TriplePattern
//import com.signalcollect.triplerush.Mapping
//import akka.event.Logging
//import com.signalcollect.triplerush.QuerySpecification
//import scala.collection.mutable.UnrolledBuffer
//import java.lang.management.ManagementFactory
//import collection.JavaConversions._
//import language.postfixOps
//import com.signalcollect.triplerush.TripleRush
//import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
//
//object LubmBenchmark extends App {
//  def jvmParameters = " -Xmx31000m" +
//    " -Xms31000m" +
//    " -XX:+AggressiveOpts" +
//    " -XX:+AlwaysPreTouch" +
//    " -XX:+UseNUMA" +
//    " -XX:-UseBiasedLocking" +
//    " -XX:MaxInlineSize=1024"
//
//  def assemblyPath = "./target/scala-2.10/triplerush-assembly-1.0-SNAPSHOT.jar"
//  val assemblyFile = new File(assemblyPath)
//  val kraken = new TorqueHost(
//    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
//    localJarPath = assemblyPath, jvmParameters = jvmParameters, jdkBinPath = "/home/user/stutz/jdk1.7.0/bin/", priority = TorquePriority.fast)
//  val localHost = new LocalHost
//  val googleDocs = new GoogleDocsResultHandler(args(0), args(1), "triplerush", "data")
//
//  def getRevision: String = {
//    try {
//      val gitLogPath = ".git/logs/HEAD"
//      val gitLog = new File(gitLogPath)
//      val lines = Source.fromFile(gitLogPath).getLines
//      val lastLine = lines.toList.last
//      val revision = lastLine.split(" ")(1)
//      revision
//    } catch {
//      case t: Throwable => "Unknown revision."
//    }
//  }
//
//  /*********/
//  def evalName = s"LUBM KRAKEN Rewired index experiment."
//  def runs = 1
//  var evaluation = new Evaluation(evaluationName = evalName, executionHost = kraken).addResultHandler(googleDocs)
//  //    var evaluation = new Evaluation(evaluationName = evalName, executionHost = localHost).addResultHandler(googleDocs)
//  /*********/
//
//  for (unis <- List(160)) { //10, 20, 40, 80, 160, 320, 480, 800
//    for (run <- 1 to runs) {
//      for (optimizer <- List(QueryOptimizer.Clever)) {
//        evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(
//          evalName,
//          false,
//          Long.MaxValue,
//          optimizer,
//          getRevision,
//          unis))
//      }
//    }
//  }
//  evaluation.execute
//
//  def lubmBenchmarkRun(
//    description: String,
//    sampling: Boolean,
//    tickets: Long,
//    optimizer: Int,
//    revision: String,
//    universities: Int)(): List[Map[String, String]] = {
//
//    val queries = LubmQueries.fullQueries
//
//    var baseResults = Map[String, String]()
//    val krakenFromKraken = new TorqueHost(
//      jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
//      localJarPath = "/home/user/stutz/triplerush-assembly-1.0-SNAPSHOT.jar", jvmParameters = jvmParameters, jdkBinPath = "/home/user/stutz/jdk1.7.0/bin/", priority = TorquePriority.fast)
//    //      
//    val numberOfNodes = 8
//    val graphBuilder = GraphBuilder.
//      //      withLoggingLevel(Logging.DebugLevel).
//      withNodeProvisioner(new TorqueNodeProvisioner(krakenFromKraken, numberOfNodes, allocateWorkersOnCoordinatorNode = true, copyExecutable = false))
//    val qe = new TripleRush(graphBuilder)
//    def loadLubm {
//      val lubmFolderName = s"lubm$universities-filtered-splits"
//      for (splitId <- 0 until 2880) {
//        val splitFile = s"./$lubmFolderName/$splitId.filtered-split"
//        qe.loadBinary(splitFile, Some(splitId))
//        if (splitId % 288 == 279) {
//          println(s"Dispatched up to split #$splitId/2880, awaiting idle.")
//          qe.awaitIdle
//          println(s"Continuing graph loading...")
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
//    def jitRepetitions = 100
//
//    /**
//     * Go to JVM JIT steady state by executing the queries multiple times.
//     */
//    def jitSteadyState {
//      for (i <- 1 to jitRepetitions) {
//        for (queryId <- 1 to 7) {
//          val queryIndex = queryId - 1
//          val query = queries(queryIndex).toParticle
//          print(s"Warming up with query ${new QueryParticle(query).queryId} ...")
//          qe.executeQuery(query)
//          qe.awaitIdle
//          println(s" Done.")
//        }
//      }
//    }
//
//    lazy val gcs = ManagementFactory.getGarbageCollectorMXBeans
//
//    def getGcCollectionTime: Long = {
//      gcs map (_.getCollectionTime) sum
//    }
//
//    def lastGcId: Long = {
//      val sunGcs = gcs map (_.asInstanceOf[com.sun.management.GarbageCollectorMXBean])
//      val gcIds = sunGcs.
//        map(_.getLastGcInfo).
//        flatMap(info => if (info != null) Some(info.getId) else None)
//      if (gcIds.isEmpty) 0 else gcIds.max
//    }
//
//    def freedDuringLastGc: Long = {
//      val sunGcs = gcs map (_.asInstanceOf[com.sun.management.GarbageCollectorMXBean])
//      val usedBeforeLastGc = sunGcs.
//        map(_.getLastGcInfo).
//        map(_.getMemoryUsageBeforeGc).
//        flatMap(_.values).
//        map(_.getCommitted).
//        sum
//      val usedAfterLastGc = sunGcs.
//        map(_.getLastGcInfo).
//        map(_.getMemoryUsageAfterGc).
//        flatMap(_.values).
//        map(_.getCommitted).
//        sum
//      val freedDuringLastGc = usedBeforeLastGc - usedAfterLastGc
//      freedDuringLastGc
//    }
//
//    def getGcCollectionCount: Long = {
//      gcs map (_.getCollectionCount) sum
//    }
//
//    lazy val compilations = ManagementFactory.getCompilationMXBean
//
//    lazy val javaVersion = ManagementFactory.getRuntimeMXBean.getVmVersion
//
//    lazy val jvmLibraryPath = ManagementFactory.getRuntimeMXBean.getLibraryPath
//
//    lazy val jvmArguments = ManagementFactory.getRuntimeMXBean.getInputArguments
//
//    def cleanGarbage {
//      for (i <- 1 to 10) {
//        System.runFinalization
//        System.gc
//        Thread.sleep(10000)
//      }
//      Thread.sleep(120000)
//    }
//
//    var finalResults = List[Map[String, String]]()
//    def runEvaluation(queryId: Int) {
//      var runResult = baseResults
//      var date: Date = new Date
//      val queryIndex = queryId - 1
//      val query = queries(queryIndex)
//      val particle = query.toParticle
//      val gcTimeBefore = getGcCollectionTime
//      val gcCountBefore = getGcCollectionCount
//      val compileTimeBefore = compilations.getTotalCompilationTime
//      runResult += ((s"totalMemoryBefore", bytesToGigabytes(Runtime.getRuntime.totalMemory).toString))
//      runResult += ((s"freeMemoryBefore", bytesToGigabytes(Runtime.getRuntime.freeMemory).toString))
//      runResult += ((s"usedMemoryBefore", bytesToGigabytes(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory).toString))
//      val startTime = System.nanoTime
//      val (queryResultFuture, queryStatsFuture) = qe.executeAdvancedQuery(particle)
//      val queryResult = Await.result(queryResultFuture, 7200 seconds)
//      val finishTime = System.nanoTime
//      val executionTime = roundToMillisecondFraction(finishTime - startTime)
//      val gcTimeAfter = getGcCollectionTime
//      val gcCountAfter = getGcCollectionCount
//      val gcTimeDuringQuery = gcTimeAfter - gcTimeBefore
//      val gcCountDuringQuery = gcCountAfter - gcCountBefore
//      val compileTimeAfter = compilations.getTotalCompilationTime
//      val compileTimeDuringQuery = compileTimeAfter - compileTimeBefore
//      val queryStats = Await.result(queryStatsFuture, 10 seconds)
//      val optimizingTime = roundToMillisecondFraction(queryStats("optimizingDuration").asInstanceOf[Long])
//      runResult += ((s"revision", revision))
//      runResult += ((s"queryId", queryId.toString))
//      runResult += ((s"optimizer", optimizer.toString))
//      runResult += ((s"queryCopyCount", queryStats("queryCopyCount").toString))
//      runResult += ((s"query", queryStats("optimizedQuery").toString))
//      runResult += ((s"exception", queryStats("exception").toString))
//      runResult += ((s"results", queryResult.length.toString))
//      runResult += ((s"executionTime", executionTime.toString))
//      runResult += ((s"optimizingTime", optimizingTime.toString))
//      runResult += ((s"totalMemory", bytesToGigabytes(Runtime.getRuntime.totalMemory).toString))
//      runResult += ((s"freeMemory", bytesToGigabytes(Runtime.getRuntime.freeMemory).toString))
//      runResult += ((s"usedMemory", bytesToGigabytes(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory).toString))
//      runResult += ((s"executionHostname", java.net.InetAddress.getLocalHost.getHostName))
//      runResult += (("gcTimeAfter", gcTimeAfter.toString))
//      runResult += (("gcCountAfter", gcCountAfter.toString))
//      runResult += (("gcTimeDuringQuery", gcTimeDuringQuery.toString))
//      runResult += (("gcCountDuringQuery", gcCountDuringQuery.toString))
//      runResult += (("compileTimeAfter", compileTimeAfter.toString))
//      runResult += (("compileTimeDuringQuery", compileTimeDuringQuery.toString))
//      runResult += s"loadNumber" -> universities.toString
//      runResult += s"date" -> date.toString
//      runResult += s"dataSet" -> s"lubm$universities"
//      finalResults = runResult :: finalResults
//    }
//
//    def bytesToGigabytes(bytes: Long): Double = ((bytes / 1073741824.0) * 10.0).round / 10.0
//
//    baseResults += (("evaluationDescription", description))
//    baseResults += (("jitRepetitions", jitRepetitions.toString))
//    baseResults += (("java.runtime.version", System.getProperty("java.runtime.version")))
//    baseResults += (("javaVmVersion", javaVersion))
//    baseResults += (("jvmLibraryPath", jvmLibraryPath))
//    baseResults += (("jvmArguments", jvmArguments.mkString(" ")))
//
//    val loadingTime = measureTime {
//      println("Dispatching loading command to workers...")
//      loadLubm
//      qe.prepareExecution
//    }
//    baseResults += (("loadingTime", loadingTime.toString))
//
//    val loadingTime = measureTime {
//      println("Dispatching loading command to workers...")
//      loadLubm
//      qe.prepareExecution
//    }
//
//    runResult += s"loadNumber" -> universities.toString
//    runResult += s"dataSet" -> s"lubm$universities"
//    baseResults += (("loadingTime", loadingTime.toString))
//
//    println("Starting warm-up...")
//    jitSteadyState(queries, tr, warmupRepetitions)
//    cleanGarbage
//    println(s"Finished warm-up.")
//    for (queryId <- 1 to 7) {
//      println(s"Running evaluation for query $queryId.")
//      runEvaluation(queryId)
//      println(s"Done running evaluation for query $queryId. Awaiting idle")
//      qe.awaitIdle
//      println("Idle")
//    }
//    tr.shutdown
//    finalResults
//  }
//}