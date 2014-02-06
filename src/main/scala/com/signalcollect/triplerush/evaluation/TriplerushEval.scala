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

import java.io.File
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Random
import com.signalcollect.GraphBuilder
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.nodeprovisioning.torque.LocalHost
import com.signalcollect.nodeprovisioning.torque.TorqueHost
import com.signalcollect.nodeprovisioning.torque.TorqueJobSubmitter
import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
import com.signalcollect.nodeprovisioning.torque.TorquePriority
import com.signalcollect.triplerush.Mapping
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.QueryEngine
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.Mapping
import akka.event.Logging
import com.signalcollect.triplerush.QuerySpecification
import scala.collection.mutable.UnrolledBuffer
import java.lang.management.ManagementFactory
import collection.JavaConversions._
import language.postfixOps
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
import collection.JavaConversions._
import java.lang.management.GarbageCollectorMXBean
import com.signalcollect.nodeprovisioning.NodeProvisioner
import com.signalcollect.triplerush.optimizers.Optimizer

trait TriplerushEval {

  def description: String
  def numberOfNodes: Int
  def warmupRepetitions: Int
  def optimizerCreator: TripleRush => Option[Optimizer]
  def revision: String
  def torquePriority: String

  import EvalHelpers._

  def evaluationRun: List[Map[String, String]]

  def initializeGraphBuilder: GraphBuilder[Any, Any] = {
    GraphBuilder.
      withNodeProvisioner(initializeTorqueNodeProvisioner)
  }

  def initializeTorqueNodeProvisioner: NodeProvisioner = {
    new TorqueNodeProvisioner(
      krakenFromKraken,
      numberOfNodes,
      allocateWorkersOnCoordinatorNode = true,
      copyExecutable = false)
  }

  def initializeTr(gb: GraphBuilder[Any, Any]): TripleRush = {
    assert(gb != null)
    new TripleRush(gb)
  }

  def baseStats = Map[String, String](
    ("evaluationDescription", description),
    ("numberOfNodes", numberOfNodes.toString),
    ("jitRepetitions", warmupRepetitions.toString),
    ("java.runtime.version", System.getProperty("java.runtime.version")))

  def krakenFromKraken = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    coresPerNode = 23,
    localJarPath = "/home/user/stutz/triplerush-assembly-1.0-SNAPSHOT.jar", jvmParameters = jvmParameters, jdkBinPath = "/home/user/stutz/jdk1.7.0/bin/", priority = torquePriority)

  def runEvaluation(query: QuerySpecification, queryDescription: String, optimizer: Option[Optimizer], tr: TripleRush, commonResults: Map[String, String]): Map[String, String] = {
    val gcs = ManagementFactory.getGarbageCollectorMXBeans.toList
    val compilations = ManagementFactory.getCompilationMXBean
    val javaVersion = ManagementFactory.getRuntimeMXBean.getVmVersion
    val jvmLibraryPath = ManagementFactory.getRuntimeMXBean.getLibraryPath
    val jvmArguments = ManagementFactory.getRuntimeMXBean.getInputArguments
    var runResult = commonResults
    runResult += (("javaVmVersion", javaVersion))
    runResult += (("jvmLibraryPath", jvmLibraryPath))
    runResult += (("jvmArguments", jvmArguments.mkString(" ")))
    val date: Date = new Date
    val gcTimeBefore = getGcCollectionTime(gcs)
    val gcCountBefore = getGcCollectionCount(gcs)
    val compileTimeBefore = compilations.getTotalCompilationTime
    runResult += ((s"totalMemoryBefore", bytesToGigabytes(Runtime.getRuntime.totalMemory).toString))
    runResult += ((s"freeMemoryBefore", bytesToGigabytes(Runtime.getRuntime.freeMemory).toString))
    runResult += ((s"usedMemoryBefore", bytesToGigabytes(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory).toString))
    val startTime = System.nanoTime
    val (queryResultFuture, queryStatsFuture) = tr.executeAdvancedQuery(query, optimizer)
    val queryResult = Await.result(queryResultFuture, 7200 seconds)
    val finishTime = System.nanoTime
    val executionTime = roundToMillisecondFraction(finishTime - startTime)
    val gcTimeAfter = getGcCollectionTime(gcs)
    val gcCountAfter = getGcCollectionCount(gcs)
    val gcTimeDuringQuery = gcTimeAfter - gcTimeBefore
    val gcCountDuringQuery = gcCountAfter - gcCountBefore
    val compileTimeAfter = compilations.getTotalCompilationTime
    val compileTimeDuringQuery = compileTimeAfter - compileTimeBefore
    val queryStats = Await.result(queryStatsFuture, 10 seconds)
    val optimizingTime = roundToMillisecondFraction(queryStats("optimizingDuration").asInstanceOf[Long])
    runResult += ((s"revision", revision))
    runResult += ((s"queryId", queryDescription))
    runResult += ((s"optimizer", optimizer.toString))
    runResult += ((s"queryCopyCount", queryStats("queryCopyCount").toString))
    runResult += ((s"query", queryStats("optimizedQuery").toString))
    runResult += ((s"exception", queryStats("exception").toString))
    runResult += ((s"results", queryResult.size.toString))
    runResult += ((s"executionTime", executionTime.toString))
    runResult += ((s"optimizingTime", optimizingTime.toString))
    runResult += ((s"totalMemory", bytesToGigabytes(Runtime.getRuntime.totalMemory).toString))
    runResult += ((s"freeMemory", bytesToGigabytes(Runtime.getRuntime.freeMemory).toString))
    runResult += ((s"usedMemory", bytesToGigabytes(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory).toString))
    runResult += ((s"executionHostname", java.net.InetAddress.getLocalHost.getHostName))
    runResult += (("gcTimeAfter", gcTimeAfter.toString))
    runResult += (("gcCountAfter", gcCountAfter.toString))
    runResult += (("gcTimeDuringQuery", gcTimeDuringQuery.toString))
    runResult += (("gcCountDuringQuery", gcCountDuringQuery.toString))
    runResult += (("compileTimeAfter", compileTimeAfter.toString))
    runResult += (("compileTimeDuringQuery", compileTimeDuringQuery.toString))
    runResult += s"date" -> date.toString
    runResult
  }

}

object EvalHelpers {

  def jvmParameters = " -Xmx31000m" +
    " -Xms31000m" +
    " -XX:+AggressiveOpts" +
    " -XX:+AlwaysPreTouch" +
    " -XX:+UseNUMA" +
    " -XX:-UseBiasedLocking" +
    " -XX:MaxInlineSize=1024"

  def assemblyPath = "./target/scala-2.10/triplerush-assembly-1.0-SNAPSHOT.jar"
  def assemblyFile = new File(assemblyPath)
  def kraken(torquePriority: String) = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    coresPerNode = 23,
    localJarPath = assemblyPath, jvmParameters = jvmParameters, jdkBinPath = "/home/user/stutz/jdk1.8.0/bin/", priority = torquePriority)
  def localHost = new LocalHost

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

  def bytesToGigabytes(bytes: Long): Double = ((bytes / 1073741824.0) * 10.0).round / 10.0

  def cleanGarbage {
    for (i <- 1 to 10) {
      System.runFinalization
      System.gc
      Thread.sleep(10000)
    }
    Thread.sleep(120000)
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

  /**
   * Go to JVM JIT steady state by executing the queries multiple times.
   */
  def jitSteadyState(queries: List[QuerySpecification], optimizer: Option[Optimizer], tr: TripleRush, repetitions: Int = 100) {
    for (i <- 1 to repetitions) {
      println(s"running warmup $i/$repetitions")
      for (query <- queries) {
        tr.executeQuery(query, optimizer)
        tr.awaitIdle
      }
    }
    println(s"warmup finished")
  }

  def getGcCollectionTime(gcs: List[GarbageCollectorMXBean]): Long = {
    gcs map (_.getCollectionTime) sum
  }

  def lastGcId(gcs: List[GarbageCollectorMXBean]): Long = {
    val sunGcs = gcs map (_.asInstanceOf[com.sun.management.GarbageCollectorMXBean])
    val gcIds = sunGcs.
      map(_.getLastGcInfo).
      flatMap(info => if (info != null) Some(info.getId) else None)
    if (gcIds.isEmpty) 0 else gcIds.max
  }

  def freedDuringLastGc(gcs: List[GarbageCollectorMXBean]): Long = {
    val sunGcs = gcs map (_.asInstanceOf[com.sun.management.GarbageCollectorMXBean])
    val usedBeforeLastGc = sunGcs.
      map(_.getLastGcInfo).
      map(_.getMemoryUsageBeforeGc).
      flatMap(_.values).
      map(_.getCommitted).
      sum
    val usedAfterLastGc = sunGcs.
      map(_.getLastGcInfo).
      map(_.getMemoryUsageAfterGc).
      flatMap(_.values).
      map(_.getCommitted).
      sum
    val freedDuringLastGc = usedBeforeLastGc - usedAfterLastGc
    freedDuringLastGc
  }

  def getGcCollectionCount(gcs: List[GarbageCollectorMXBean]): Long = {
    gcs map (_.getCollectionCount) sum
  }
}