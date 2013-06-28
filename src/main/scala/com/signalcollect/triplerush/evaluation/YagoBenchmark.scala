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
//object YagoBenchmark extends App {
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
//  def evalName = "Yago eval."
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
//      /**
//       * Queries from: https://domino.mpi-inf.mpg.de/intranet/ag5/ag5publ.nsf/0/AD3DBAFA6FB90DD2C1257593002FF3DF/$file/rdf3x.pdf
//       * 
//       * Yago Dataset. We grouped the queries thematically into three groups. The first
//	   * group consists of oriented facts, e.g.: "scientists from Switzerland with a
//       * doctoral advisor from Germany" (A1). The second group is relationship oriented,
//       * e.g. "two actors from England playing together in the same movie" (B1). The
//       * third group examines relationships with unknown predicates, e.g. "two scientists
//       * related to the same city" (C1).
//       * A1: select ?gn ?fn where f ?gn <givenNameOf> ?p. ?fn <familyNameOf> ?p.
//       * ?p <type> "scientist"; <bornInLocation> ?city; <hasDoctoralAdvisor> ?a. ?a
//       * <bornInLocation> ?city2. ?city <locatedIn> "Switzerland". ?city2 <locatedIn>
//       * "Germany". g
//       * A2: select ?n where f ?a <isCalled> ?n; <type> "actor"; <livesIn> ?city;
//       * <actedIn> ?m1; <directed> ?m2. ?city <locatedIn> ?s. ?s <locatedIn>
//       * "United States". ?m1 <type> "movie"; <producedInCountry> "Germany". ?m2
//       * 51<type> "movie"; <producedInCountry> "Canada". g
//       * A3: select distinct ?n ?co where f ?p <isCalled> ?n. f ?p <type> "actor" g
//       * union f ?p <type> "athlete" g ?p <bornInLocation> ?c. ?c <locatedIn> ?s. ?s
//       * <locatedIn> ?co. ?p <type> ?t. Filter(?t reaches "politician" via <subClassOf>)
//       * g
//       * B1: select distinct ?n1 ?n2 where f ?a1 <isCalled> ?n1; <livesIn> ?c1;
//       * <actedIn> ?movie. ?a2 <isCalled> ?n2; <livesIn> ?c2; <actedIn> ?movie. ?c1
//       * <locatedIn> "England". ?c2 <locatedIn> "England". Filter (?a1 != ?a2) g
//       * B2: select ?n1 ?n2 where f ?p1 <isCalled> ?n1; <bornInLocation> ?city;
//       * <isMarriedTo> ?p2. ?p2 <isCalled> ?n2; <bornInLocation> ?city. g
//       * B3: select distinct ?n1 ?n2 where f ?n1 <familyNameOf> ?p1. ?n2 <family-
//       * NameOf> ?p2. ?p1 <type> "scientist"; <hasWonPrize> ?award; <bornInLoca-
//       * tion> ?city. ?p2 <type> "scientist"; <hasWonPrize> ?award; <bornInLocation>
//       * ?city. Filter (?p1 != ?p2) g
//       * C1: select distinct ?n1 ?n2 where f?n1 <familyNameOf> ?p1. ?n2 <family-
//       * NameOf> ?p2. ?p1 <type> "scientist"; [] ?city. ?p2 <type> "scientist"; [] ?city.
//       * ?city <type> <site> Filter (?p1 != ?p2) g
//       * C2: select distinct ?n where f ?p <isCalled> ?n; [] ?c1. [] ?c2. ?c1 <type>
//       * <village>; <isCalled> "London". ?c2 <type> <site>; <isCalled> "Paris". g
//       */
//      def fullQueries: List[QueryParticle] = List(
//        QueryParticle(queryId = 1,
//          unmatched = Array(TriplePattern(-1, 2, 2009), TriplePattern(-1, 18, -2), TriplePattern(-1, 411, -3), TriplePattern(-3, 2, 7), TriplePattern(-3, 9, -2), TriplePattern(-2, 2, 3)),
//          bindings = new Array(3)),
//        QueryParticle(2, Array(TriplePattern(-1, 2, 3063), TriplePattern(-1, 4, -2)),
//          bindings = new Array(2)),
//        QueryParticle(3, Array(TriplePattern(-1, 18, -2), TriplePattern(-1, 2, 409), TriplePattern(-1, 411, -3), TriplePattern(-3, 9, -2), TriplePattern(-3, 2, 7), TriplePattern(-2, 2, 3)),
//          bindings = new Array(3)),
//        QueryParticle(4, Array(TriplePattern(-1, 23, 6), TriplePattern(-1, 2, 11), TriplePattern(-1, 4, -2), TriplePattern(-1, 24, -3), TriplePattern(-1, 26, -4)),
//          bindings = new Array(4)),
//        QueryParticle(5, Array(TriplePattern(-1, 9, 6), TriplePattern(-1, 2, 2571)),
//          bindings = new Array(1)),
//        QueryParticle(6, Array(TriplePattern(-1, 9, 1), TriplePattern(-1, 2, 7), TriplePattern(-2, 23, -1), TriplePattern(-2, 2, 11)),
//          bindings = new Array(2)),
//        QueryParticle(7, Array(TriplePattern(-1, 2, 11), TriplePattern(-1, 13, -2), TriplePattern(-2, 2, 3063), TriplePattern(-3, 426, -1), TriplePattern(-3, 413, -2), TriplePattern(-3, 2, 409)),
//          bindings = new Array(3)))
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
//      //            withLoggingLevel(Logging.DebugLevel).
//      //      withConsole(true, 8080).
//      //      withNodeProvisioner(new TorqueNodeProvisioner(
//      //        torqueHost = new TorqueHost(
//      //          jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
//      //          localJarPath = assemblyPath,
//      //          jvmParameters = jvmHighThroughputGc,
//      //          priority = TorquePriority.fast),
//      //        numberOfNodes = 10)))
//
//      def loadSmallLubm {
//        val smallLubmFolderName = "lubm160-filtered-splits"
//        for (splitId <- 0 until 2880) {
//          qe.loadBinary(s"./$smallLubmFolderName/$splitId.filtered-split", Some(splitId))
//          if (splitId % 288 == 279) {
//            println(s"Dispatched up to split #$splitId/2880, awaiting idle.")
//            qe.awaitIdle
//            println(s"Continuing graph loading...")
//          }
//        }
//        println("Query engine preparing query execution.")
//        qe.prepareQueryExecution
//        println("Query engine ready.")
//      }
//
//      def loadLargeLubm {
//        val largeLubmFolderName = "/home/torque/tmp/lubm10240-filtered-splits"
//        for (splitId <- 0 until 2880) {
//          qe.loadBinary(s"$largeLubmFolderName/$splitId.filtered-split", Some(splitId))
//          if (splitId % 288 == 279) {
//            println(s"Dispatched up to split #$splitId/2880, awaiting idle.")
//            qe.awaitIdle
//            println(s"Continuing graph loading..")
//          }
//        }
//        println("Query engine preparing query execution")
//        qe.prepareQueryExecution
//      }
//
//      /**
//       * Returns the time in milliseconds it takes to execute the code in 'codeBlock'.
//       */
//      def measureTime(codeBlock: => Unit): Long = {
//        val startTime = System.currentTimeMillis
//        codeBlock
//        val finishTime = System.currentTimeMillis
//        finishTime - startTime
//      }
//
//      def roundToMillisecondFraction(nanoseconds: Long): Double = {
//        ((nanoseconds / 100000.0).round) / 10.0
//      }
//
//      def executeOnQueryEngine(q: QueryParticle): QueryResult = {
//        val resultFuture = qe.executeQuery(q, optimizer)
//        try {
//          Await.result(resultFuture, new FiniteDuration(1000, TimeUnit.SECONDS)) // TODO handle exception
//        } catch {
//          case t: Throwable =>
//            println(s"Query $q timed out!")
//            QueryResult(List(), Array("exception"), Array(t))
//        }
//      }
//
//      /**
//       * Go to JVM JIT steady state by executing the query 100 times.
//       */
//      def jitSteadyState {
//        for (i <- 1 to 5) {
//          for (queryId <- 1 to 7) {
//            val queryIndex = queryId - 1
//            val query = fullQueries(queryIndex)
//            print(s"Warming up with query $query ...")
//            executeOnQueryEngine(query)
//            qe.awaitIdle
//            println(s" Done.")
//          }
//        }
//      }
//
//      def cleanGarbage {
//        for (i <- 1 to 10) {
//          System.gc
//          Thread.sleep(100)
//        }
//        Thread.sleep(10000)
//      }
//
//    var finalResults = List[Map[String, String]]()
//      def runEvaluation(queryId: Int) {
//        var runResult = baseResults
//        var date: Date = new Date
//        val queryIndex = queryId - 1
//        val query = queries(queryIndex)
//        val startTime = System.nanoTime
//        val queryResult = executeOnQueryEngine(query)
//        val queryStats: Map[Any, Any] = (queryResult.statKeys zip queryResult.statVariables).toMap.withDefaultValue("")
//        val finishTime = System.nanoTime
//        val executionTime = roundToMillisecondFraction(finishTime - startTime)
//        val timeToFirstResult = roundToMillisecondFraction(queryStats("firstResultNanoTime").asInstanceOf[Long] - startTime)
//        val optimizingTime = roundToMillisecondFraction(queryStats("optimizingDuration").asInstanceOf[Long])
//        runResult += s"revision" -> revision
//        runResult += s"queryId" -> queryId.toString
//        runResult += s"optimizer" -> optimizer.toString
//        runResult += s"queryCopyCount" -> queryStats("queryCopyCount").toString
//        runResult += s"query" -> queryStats("optimizedQuery").toString
//        runResult += s"exception" -> queryStats("exception").toString
//        runResult += s"results" -> queryResult.queries.length.toString
//        runResult += s"samplingQuery" -> query.isSamplingQuery.toString
//        runResult += s"tickets" -> query.tickets.toString
//        runResult += s"executionTime" -> executionTime.toString
//        runResult += s"timeUntilFirstResult" -> timeToFirstResult.toString
//        runResult += s"optimizingTime" -> optimizingTime.toString
//        runResult += s"totalMemory" -> bytesToGigabytes(Runtime.getRuntime.totalMemory).toString
//        runResult += s"freeMemory" -> bytesToGigabytes(Runtime.getRuntime.freeMemory).toString
//        runResult += s"usedMemory" -> bytesToGigabytes(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory).toString
//        runResult += s"executionHostname" -> java.net.InetAddress.getLocalHost.getHostName
//        runResult += s"loadNumber" -> 160.toString
//        runResult += s"date" -> date.toString
//        finalResults = runResult :: finalResults
//      }
//
//      def bytesToGigabytes(bytes: Long): Double = ((bytes / 1073741824.0) * 10.0).round / 10.0
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