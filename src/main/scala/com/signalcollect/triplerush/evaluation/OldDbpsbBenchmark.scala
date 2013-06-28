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
//import com.signalcollect.triplerush.QueryOptimizer
//import com.signalcollect.triplerush.TriplePattern
//import com.signalcollect.triplerush.Mapping
//import akka.event.Logging
//import com.signalcollect.triplerush.QueryResult
//
///**
// * Runs a PageRank algorithm on a graph of a fixed size
// * for different numbers of worker threads.
// *
// * Evaluation is set to execute on a 'Kraken'-node.
// */
//object OldDbpsbBenchmark extends App {
//  def jvmHighThroughputGc = " -Xmx64000m" +
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
//    " -XX:-PrintCompilation" +
//    " -XX:-PrintGC" +
//    " -Dsun.io.serialization.extendedDebugInfo=true" +
//    " -XX:MaxInlineSize=1024"
//
//  def jvmParameters = " -Xmx64000m" +
//    " -Xms64000m"
//  def assemblyPath = "./target/scala-2.10/triplerush-assembly-1.0-SNAPSHOT.jar"
//  val assemblyFile = new File(assemblyPath)
//  //  val jobId = Random.nextInt % 10000
//  //  def copyName = assemblyPath.replace("-SNAPSHOT", jobId.toString)
//  //  assemblyFile.renameTo(new File(assemblyPath))
//  val kraken = new TorqueHost(
//    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
//    localJarPath = assemblyPath, jvmParameters = jvmHighThroughputGc, priority = TorquePriority.superfast)
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
//  def evalName = "DBPSB eval."
//  //  def evalName = "Local debugging."
//  def runs = 1
//  var evaluation = new Evaluation(evaluationName = evalName, executionHost = kraken).addResultHandler(googleDocs)
//  //  var evaluation = new Evaluation(evaluationName = evalName, executionHost = localHost).addResultHandler(googleDocs)
//  /*********/
//
//  for (run <- 1 to runs) {
//    // for (queryId <- 1 to 1) {
//    for (optimizer <- List(QueryOptimizer.Clever)) {
//      //for (tickets <- List(1000, 10000, 100000, 1000000)) {
//      //evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, queryId, true, tickets))
//      //        evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(evalName, queryId, false, tickets))
//      //      }
//      evaluation = evaluation.addEvaluationRun(lubmBenchmarkRun(
//        evalName,
//        //queryId,
//        false,
//        Long.MaxValue,
//        optimizer,
//        getRevision))
//    }
//    //  }
//  }
//  evaluation.execute
//
//  def lubmBenchmarkRun(
//    description: String,
//    //queryId: Int,
//    sampling: Boolean,
//    tickets: Long,
//    optimizer: Int,
//    revision: String)(): List[Map[String, String]] = {
//
//    /**
//     * Queries from Trinity.RDF paper
//     *
//     *
//     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
//     * SELECT ?title
//     * WHERE {
//     *    ?game <http://www.w3.org/2004/02/skos/core#subject> <http://dbpedia.org/resource/Category:First-person_shooters> .
//     *    ?game foaf:name ?title .
//     * }
//     * -----
//     * SELECT ?var
//     * WHERE {
//     * 	?var3 <http://xmlns.com/foaf/0.1/homepage> ?var2 .
//     * 	?var3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var .
//     * }
//     * ------
//     *
//     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
//     * SELECT ?name ?description ?musician WHERE {
//     *      ?musician <http://www.w3.org/2004/02/skos/core#subject> <http://dbpedia.org/resource/Category:German_musicians> .
//     *      ?musician foaf:name ?name .
//     *      ?musician rdfs:comment ?description .
//     * }
//     * -------
//     * PREFIX dbo: <http://dbpedia.org/ontology/>
//     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
//     * SELECT ?name ?birth ?death ?person WHERE {
//     * ?person dbo:birthPlace <http://dbpedia.org/resource/Berlin> .
//     * ?person dbo:birthDate ?birth .
//     * ?person foaf:name ?name .
//     * ?person dbo:deathDate ?death .
//     * }
//     * -----
//     *
//     * PREFIX dbo: <http://dbpedia.org/ontology/>
//     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
//     * SELECT ?manufacturer ?name ?car
//     * WHERE {
//     * ?car <http://www.w3.org/2004/02/skos/core#subject> <http://dbpedia.org/resource/Category:Luxury_vehicles> .
//     * ?car foaf:name ?name .
//     * ?car dbo:manufacturer ?man .
//     * ?man foaf:name ?manufacturer
//     * }
//     *
//     * ---------
//     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
//     * PREFIX dbpprop:<http://dbpedia.org/property/>
//     * SELECT ?var0 ?var1 ?var2 ?var3 where
//     * {
//     * ?var6 rdf:type ?var.
//     * ?var6 dbpprop:name ?var0.
//     * ?var6 dbpprop:pages ?var1.
//     * ?var6 dbpprop:isbn ?var2.
//     * ?var6 dbpprop:author ?var3.
//     * }
//     * -----
//     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
//     * PREFIX dbpprop:<http://dbpedia.org/property/>
//     * SELECT ?var where
//     * {
//     * ?var6 rdf:type ?var.
//     * ?var6 dbpprop:name ?var0.
//     * ?var6 dbpprop:pages ?var1.
//     * ?var6 dbpprop:isbn ?var2.
//     * ?var6 dbpprop:author ?var3.
//     * }
//     * -----
//     *
//     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
//     * PREFIX dbpedia2: <http://dbpedia.org/property/>
//     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
//     * SELECT ?player WHERE {
//     * ?s foaf:page ?player .
//     * ?s rdf:type <http://dbpedia.org/ontology/SoccerPlayer> .
//     * ?s dbpedia2:position ?position .
//     * ?s <http://dbpedia.org/property/clubs> ?club .
//     * ?club <http://dbpedia.org/ontology/capacity> ?cap .
//     * ?s <http://dbpedia.org/ontology/birthPlace> ?place .
//     * ?place <http://dbpedia.org/property/population> ?pop .
//     * ?s <http://dbpedia.org/ontology/number> ?tricot .
//     * }
//     *
//     * http://www.w3.org/2004/02/skos/core#subject -> 1
//     * http://dbpedia.org/resource/Category:First-person_shooters -> 47406
//     * http://xmlns.com/foaf/0.1/name -> 41
//     * http://xmlns.com/foaf/0.1/homepage -> 653
//     * http://www.w3.org/1999/02/22-rdf-syntax-ns#type -> 16
//     * http://dbpedia.org/resource/Category:German_musicians -> 187543
//     * http://www.w3.org/2000/01/rdf-schema#comment -> 27
//     * http://dbpedia.org/ontology/birthPlace -> 1132
//     * http://dbpedia.org/resource/Berlin -> 19706
//     * http://dbpedia.org/ontology/birthDate -> 436
//     * http://dbpedia.org/ontology/deathDate -> 1177
//     * http://dbpedia.org/resource/Category:Luxury_vehicles -> 322352
//     * http://dbpedia.org/ontology/manufacturer -> 11736
//     * http://www.w3.org/1999/02/22-rdf-syntax-ns#type -> 16
//     * http://dbpedia.org/property/name -> 30
//     * http://dbpedia.org/property/pages -> 37409
//     * http://dbpedia.org/property/isbn -> 3385
//     * http://dbpedia.org/property/author -> 3371
//     * http://xmlns.com/foaf/0.1/page -> 39
//     * http://dbpedia.org/ontology/SoccerPlayer -> 1723
//     * http://dbpedia.org/property/position -> 397
//     * http://dbpedia.org/property/clubs -> 1709
//     * http://dbpedia.org/ontology/capacity -> 6306
//     * http://dbpedia.org/property/population -> 966
//     * http://dbpedia.org/ontology/number -> 411
//     */
//    def fullQueries: List[QueryParticle] = List(
//      QueryParticle(queryId = 1,
//        unmatched = Array(TriplePattern(-1, 1, 47406), TriplePattern(-1, 41, -2)),
//        bindings = new Array(2)),
//      QueryParticle(2, Array(TriplePattern(-1, 653, -2), TriplePattern(-1, 16, -3)),
//        bindings = new Array(3)),
//      QueryParticle(3, Array(TriplePattern(-1, 1, 187543), TriplePattern(-1, 41, -2), TriplePattern(-1, 27, -3)),
//        bindings = new Array(3)),
//      QueryParticle(4, Array(TriplePattern(-1, 1132, 19706), TriplePattern(-1, 436, -2), TriplePattern(-1, 41, -3), TriplePattern(-1, 1177, -4)),
//        bindings = new Array(4)),
//      QueryParticle(5, Array(TriplePattern(-1, 1, 322352), TriplePattern(-1, 41, -2), TriplePattern(-1, 11736, -3), TriplePattern(-3, 41, -4)),
//        bindings = new Array(4)),
//      QueryParticle(6, Array(TriplePattern(-1, 16, -2), TriplePattern(-1, 30, -3), TriplePattern(-1, 37409, -4), TriplePattern(-1, 3385, -5), TriplePattern(-1, 3371, -6)),
//        bindings = new Array(6)),
//      QueryParticle(7, Array(TriplePattern(-1, 16, -2), TriplePattern(-1, 30, -3), TriplePattern(-1, 37409, -4), TriplePattern(-1, 3385, -5), TriplePattern(-1, 3371, -6)),
//        bindings = new Array(6)),
//      QueryParticle(8, Array(TriplePattern(-1, 39, -2), TriplePattern(-1, 16, 1723), TriplePattern(-1, 397, -3), TriplePattern(-1, 1709, -4), TriplePattern(-4, 6306, -5), TriplePattern(-1, 1132, -6), TriplePattern(-6, 966, -7), TriplePattern(-1, 411, -8)),
//        bindings = new Array(8)))
//
//    val queries = {
//      require(!sampling && tickets == Long.MaxValue)
//      fullQueries
//    }
//
//    var baseResults = Map[String, String]()
//    val qe = new QueryEngine(GraphBuilder.withMessageBusFactory(
//      new BulkAkkaMessageBusFactory(1024, false)).
//      withMessageSerialization(false).
//      withAkkaMessageCompression(true))
//    //            withLoggingLevel(Logging.DebugLevel).
//    //      withConsole(true, 8080).
//    //      withNodeProvisioner(new TorqueNodeProvisioner(
//    //        torqueHost = new TorqueHost(
//    //          jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
//    //          localJarPath = assemblyPath,
//    //          jvmParameters = jvmHighThroughputGc,
//    //          priority = TorquePriority.fast),
//    //        numberOfNodes = 10)))
//
//    def loadSmallLubm {
//      val smallLubmFolderName = "lubm160-filtered-splits"
//      for (splitId <- 0 until 2880) {
//        qe.loadBinary(s"./$smallLubmFolderName/$splitId.filtered-split", Some(splitId))
//        if (splitId % 288 == 279) {
//          println(s"Dispatched up to split #$splitId/2880, awaiting idle.")
//          qe.awaitIdle
//          println(s"Continuing graph loading...")
//        }
//      }
//      println("Query engine preparing query execution.")
//      qe.prepareQueryExecution
//      println("Query engine ready.")
//    }
//
//    def loadLargeLubm {
//      val largeLubmFolderName = "/home/torque/tmp/lubm10240-filtered-splits"
//      for (splitId <- 0 until 2880) {
//        qe.loadBinary(s"$largeLubmFolderName/$splitId.filtered-split", Some(splitId))
//        if (splitId % 288 == 279) {
//          println(s"Dispatched up to split #$splitId/2880, awaiting idle.")
//          qe.awaitIdle
//          println(s"Continuing graph loading..")
//        }
//      }
//      println("Query engine preparing query execution")
//      qe.prepareQueryExecution
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
//    def executeOnQueryEngine(q: QueryParticle): QueryResult = {
//      val resultFuture = qe.executeQuery(q, optimizer)
//      try {
//        Await.result(resultFuture, new FiniteDuration(1000, TimeUnit.SECONDS)) // TODO handle exception
//      } catch {
//        case t: Throwable =>
//          println(s"Query $q timed out!")
//          QueryResult(List(), Array("exception"), Array(t))
//      }
//    }
//
//    /**
//     * Go to JVM JIT steady state by executing the query 100 times.
//     */
//    def jitSteadyState {
//      for (i <- 1 to 5) {
//        for (queryId <- 1 to 7) {
//          val queryIndex = queryId - 1
//          val query = fullQueries(queryIndex)
//          print(s"Warming up with query $query ...")
//          executeOnQueryEngine(query)
//          qe.awaitIdle
//          println(s" Done.")
//        }
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
//      var date: Date = new Date
//      val queryIndex = queryId - 1
//      val query = queries(queryIndex)
//      val startTime = System.nanoTime
//      val queryResult = executeOnQueryEngine(query)
//      val queryStats: Map[Any, Any] = (queryResult.statKeys zip queryResult.statVariables).toMap.withDefaultValue("")
//      val finishTime = System.nanoTime
//      val executionTime = roundToMillisecondFraction(finishTime - startTime)
//      val timeToFirstResult = roundToMillisecondFraction(queryStats("firstResultNanoTime").asInstanceOf[Long] - startTime)
//      val optimizingTime = roundToMillisecondFraction(queryStats("optimizingDuration").asInstanceOf[Long])
//      runResult += s"revision" -> revision
//      runResult += s"queryId" -> queryId.toString
//      runResult += s"optimizer" -> optimizer.toString
//      runResult += s"queryCopyCount" -> queryStats("queryCopyCount").toString
//      runResult += s"query" -> queryStats("optimizedQuery").toString
//      runResult += s"exception" -> queryStats("exception").toString
//      runResult += s"results" -> queryResult.queries.length.toString
//      runResult += s"samplingQuery" -> query.isSamplingQuery.toString
//      runResult += s"tickets" -> query.tickets.toString
//      runResult += s"executionTime" -> executionTime.toString
//      runResult += s"timeUntilFirstResult" -> timeToFirstResult.toString
//      runResult += s"optimizingTime" -> optimizingTime.toString
//      runResult += s"totalMemory" -> bytesToGigabytes(Runtime.getRuntime.totalMemory).toString
//      runResult += s"freeMemory" -> bytesToGigabytes(Runtime.getRuntime.freeMemory).toString
//      runResult += s"usedMemory" -> bytesToGigabytes(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory).toString
//      runResult += s"executionHostname" -> java.net.InetAddress.getLocalHost.getHostName
//      runResult += s"loadNumber" -> 160.toString
//      runResult += s"date" -> date.toString
//      finalResults = runResult :: finalResults
//    }
//
//    def bytesToGigabytes(bytes: Long): Double = ((bytes / 1073741824.0) * 10.0).round / 10.0
//
//    baseResults += "evaluationDescription" -> description
//    val loadingTime = measureTime {
//      println("Dispatching loading command to worker...")
//      loadSmallLubm
//      //loadLargeLubm
//      qe.awaitIdle
//    }
//    baseResults += "loadingTime" -> loadingTime.toString
//
//    println("Starting warm-up...")
//    jitSteadyState
//    //cleanGarbage
//    println(s"Finished warm-up.")
//    for (queryId <- 1 to 7) {
//      println(s"Running evaluation for query $queryId.")
//      runEvaluation(queryId)
//      println(s"Done running evaluation for query $queryId. Awaiting idle")
//      qe.awaitIdle
//      println("Idle")
//    }
//
//    qe.shutdown
//    finalResults
//  }
//
//}