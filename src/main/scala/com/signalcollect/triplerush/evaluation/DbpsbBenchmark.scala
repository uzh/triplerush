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
//import com.signalcollect.triplerush.TripleRush
//import com.signalcollect.triplerush.vertices.QueryOptimizer
//import com.signalcollect.triplerush.TriplePattern
//import com.signalcollect.triplerush.Mapping
//import akka.event.Logging
//import com.signalcollect.triplerush.QuerySpecification
//import scala.collection.mutable.UnrolledBuffer
//import java.lang.management.ManagementFactory
//import collection.JavaConversions._
//import language.postfixOps
//
///**
// * Runs a PageRank algorithm on a graph of a fixed size
// * for different numbers of worker threads.
// *
// * Evaluation is set to execute on a 'Kraken'-node.
// */
//object DbpsbBenchmark extends App {
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
//  def evalName = s"DBPSB Evaluation."
//  def runs = 10
//  var evaluation = new Evaluation(executionHost = kraken).addResultHandler(googleDocs)
//  /*********/
//
//  for (run <- 1 to runs) {
//    for (optimizer <- List(QueryOptimizer.Clever)) {
//      evaluation = evaluation.addEvaluationRun(dbpsbBenchmarkRun(
//        evalName,
//        false,
//        Long.MaxValue,
//        optimizer,
//        getRevision))
//    }
//  }
//  evaluation.execute
//
//  def dbpsbBenchmarkRun(
//    description: String,
//    sampling: Boolean,
//    tickets: Long,
//    optimizer: Int,
//    revision: String)(): List[Map[String, String]] = {
//
//    /**
//     * Queries from: Trinity.RDF
//     *
//     * Times Trinity: 7   220 5 7 8 21 13 28
//     */
//    val game = -1
//    val title = -2
//
//    val var3 = -1
//    val var2 = -2
//    val var1 = -3
//
//    val musician = -1
//    val name = -2
//    val vdescription = -3
//
//    val person = -1
//    val birth = -2
//    val pname = -3
//    val death = -4
//
//    val car = -1
//    val man = -3
//    val manufacturer = -4
//
//    val bvar6 = -1
//    val bvar = -2
//    val bvar0 = -3
//    val bvar1 = -4
//    val bvar2 = -5
//    val bvar3 = -6
//
//    val s = -1
//    val player = -2
//    val position = -3
//    val club = -4
//    val cap = -5
//    val place = -6
//    val pop = -7
//    val tricot = -8
//
//    val m = Map(
//      ("http://www.w3.org/2004/02/skos/core#subject", 1),
//      ("http://dbpedia.org/resource/Category:First-person_shooters", 47406),
//      ("foaf:name", 41),
//      ("foaf:homepage", 653),
//      ("rdf#type", 16),
//      ("http://dbpedia.org/resource/Category:German_musicians", 187543),
//      ("rdfs#comment", 27),
//      ("dbo:birthPlace", 1132),
//      ("http://dbpedia.org/resource/Berlin", 19706),
//      ("dbo:birthDate", 436),
//      ("dbo:deathDate", 1177),
//      ("http://dbpedia.org/resource/Category:Luxury_vehicles", 322352),
//      ("dbo:manufacturer", 11736),
//      ("dbprop:name", 30),
//      ("dbprop:pages", 37409),
//      ("dbprop:isbn", 3385),
//      ("dbprop:author", 3371),
//      ("foaf:page", 39),
//      ("dbo:SoccerPlayer", 1723),
//      ("dbprop:position", 397),
//      ("dbprop:clubs", 1709),
//      ("dbo:capacity", 6306),
//      ("dbprop:population", 966),
//      ("dbo:number", 411))
//
//    /**
//     * Queries from Trinity.RDF paper
//     *
//     */
//    def fullQueries: List[QuerySpecification] = List(
//      QuerySpecification(List(
//        TriplePattern(game, m("http://www.w3.org/2004/02/skos/core#subject"), m("http://dbpedia.org/resource/Category:First-person_shooters")), //?game <http://www.w3.org/2004/02/skos/core#subject> <http://dbpedia.org/resource/Category:First-person_shooters> .
//        TriplePattern(game, m("foaf:name"), title)) //?game foaf:name ?title .
//        ),
//      QuerySpecification(List(
//        TriplePattern(var3, m("foaf:homepage"), var2), //?var3 <http://xmlns.com/foaf/0.1/homepage> ?var2 .
//        TriplePattern(var3, m("rdf#type"), var1)) //?var3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var 
//        ),
//      QuerySpecification(List(
//        TriplePattern(musician, m("http://www.w3.org/2004/02/skos/core#subject"), m("http://dbpedia.org/resource/Category:German_musicians")), //?musician <http://www.w3.org/2004/02/skos/core#subject> <http://dbpedia.org/resource/Category:German_musicians> .
//        TriplePattern(musician, m("foaf:name"), name), //?musician foaf:name ?name .
//        TriplePattern(musician, m("rdfs#comment"), vdescription)) //?musician rdfs:comment ?description
//        ),
//      QuerySpecification(List(
//        TriplePattern(person, m("dbo:birthPlace"), m("http://dbpedia.org/resource/Berlin")),
//        TriplePattern(person, m("dbo:birthDate"), birth),
//        TriplePattern(person, m("foaf:name"), pname),
//        TriplePattern(person, m("dbo:deathDate"), death))),
//      QuerySpecification(List(
//        TriplePattern(car, m("http://www.w3.org/2004/02/skos/core#subject"), m("http://dbpedia.org/resource/Category:Luxury_vehicles")),
//        TriplePattern(car, m("foaf:name"), name),
//        TriplePattern(car, m("dbo:manufacturer"), man),
//        TriplePattern(man, m("foaf:name"), manufacturer))),
//      QuerySpecification(List(
//        TriplePattern(bvar6, m("rdf#type"), bvar),
//        TriplePattern(bvar6, m("dbprop:name"), bvar0),
//        TriplePattern(bvar6, m("dbprop:pages"), bvar1),
//        TriplePattern(bvar6, m("dbprop:isbn"), bvar2),
//        TriplePattern(bvar6, m("dbprop:author"), bvar3))),
//      QuerySpecification(List(
//        TriplePattern(bvar6, m("rdf#type"), bvar),
//        TriplePattern(bvar6, m("dbprop:name"), bvar0),
//        TriplePattern(bvar6, m("dbprop:pages"), bvar1),
//        TriplePattern(bvar6, m("dbprop:isbn"), bvar2),
//        TriplePattern(bvar6, m("dbprop:author"), bvar3))),
//      QuerySpecification(List(
//        TriplePattern(s, m("foaf:page"), player),
//        TriplePattern(s, m("rdf#type"), m("dbo:SoccerPlayer")),
//        TriplePattern(s, m("dbprop:position"), position),
//        TriplePattern(s, m("dbprop:clubs"), club),
//        TriplePattern(club, m("dbo:capacity"), cap),
//        TriplePattern(s, m("dbo:birthPlace"), place),
//        TriplePattern(place, m("dbprop:population"), pop),
//        TriplePattern(s, m("dbo:number"), tricot))))
//    val queries = {
//      require(!sampling && tickets == Long.MaxValue)
//      fullQueries
//    }
//
//    var baseResults = Map[String, String]()
//    val qe = new TripleRush()
//
//    def loadDbpsb {
//      val dbpsbFolderName = s"dbpsb10-filtered-splits"
//      for (splitId <- 0 until 2880) {
//        qe.loadBinary(s"./$dbpsbFolderName/$splitId.filtered-split", Some(splitId))
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
//        for (queryId <- 1 to 8) {
//          val queryIndex = queryId - 1
//          val query = fullQueries(queryIndex)
//          print(s"Warming up with query $query ...")
//          qe.executeQuery(query.toParticle)
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
//      runResult += ((s"results", queryResult.size.toString))
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
//      runResult += ((s"loadNumber", 10.toString))
//      runResult += ((s"date", date.toString))
//      runResult += ((s"dataSet", s"dbpsb10"))
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
//      println("Dispatching loading command to worker...")
//      loadDbpsb
//      qe.awaitIdle
//    }
//    baseResults += (("loadingTime", loadingTime.toString))
//
//    println("Starting warm-up...")
//    jitSteadyState
//    cleanGarbage
//    println(s"Finished warm-up.")
//    for (queryId <- 1 to 8) {
//      println(s"Running evaluation for query $queryId.")
//      runEvaluation(queryId)
//      println(s"Done running evaluation for query $queryId. Awaiting idle")
//      qe.awaitIdle
//      println("Idle")
//    }
//    qe.shutdown
//    finalResults
//  }
//
//}
