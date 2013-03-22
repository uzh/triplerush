package com.signalcollect.triplerush

import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.promise
import org.semanticweb.yars.nx.parser.NxParser
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.GraphBuilder
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.triplerush.Expression.int2Expression
import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
import com.signalcollect.nodeprovisioning.torque.TorqueHost
import com.signalcollect.nodeprovisioning.torque.TorqueJobSubmitter
import com.signalcollect.configuration.LoggingLevel

class QueryEngine(kraken: Boolean = false) {
  private val g = {
    if (kraken) {
      val baseOptions =
        " -Xmx64000m" +
          " -Xms64000m" +
          " -Xmn8000m" +
          " -d64"
      GraphBuilder.withNodeProvisioner(new TorqueNodeProvisioner(
        torqueHost = new TorqueHost(
          jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
          localJarPath = "./target/triplerush-assembly-1.0-SNAPSHOT.jar"),
        numberOfNodes = 1, jvmParameters = baseOptions)).withLoggingLevel(LoggingLevel.Debug).build
    } else {
      GraphBuilder.build
    }
  }
  g.setUndeliverableSignalHandler { (signal, id, sourceId, graphEditor) =>
    signal match {
      case query: PatternQuery =>
        if (query.isFailed) {
        	println(s"Failed query ${query.bindings}. Could not deliver its results to its respective query vertex ${query.queryId}")
        } else {
          //println(s"Query ${query.bindings} is failing.")
          graphEditor.sendSignal(query.failed, query.queryId, None)
        }
      //println("Query could not find its query vertex: " + p.isFailed + " queryId=" + p.queryId)
      case other =>
        println(s"failed signal delivery $other of type ${other.getClass}")
    }
  }
  g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous))
  g.awaitIdle

  def load(ntriplesFilename: String, onlyTriplesAboutKnownEntities: Boolean = false, bannedPredicates: Set[String] = Set()) {
    val is = new FileInputStream(ntriplesFilename)
    val nxp = new NxParser(is)
    var triplesRead = 0
    println("Reading triples ...")
    while (nxp.hasNext) {
      val triple = nxp.next
      val predicateString = triple(1).toString
      if (!bannedPredicates.contains(predicateString)) {
        val subjectString = triple(0).toString
        val objectString = triple(2).toString
        if (!onlyTriplesAboutKnownEntities || Mapping.existsMappingForString(subjectString) || Mapping.existsMappingForString(objectString)) {
          g.addVertex(new TripleVertex(
            TriplePattern(
              Mapping.register(subjectString),
              Mapping.register(predicateString),
              Mapping.register(objectString))))
          triplesRead += 1
          if (triplesRead % 1000 == 0) {
            println("Triples read: " + triplesRead)
          }

        }
      }
    }
    print("Waiting for graph loading to finish ... ")
    g.awaitIdle
    println("done")
  }

  private val maxQueryId = new AtomicInteger

  def executeQuery(q: PatternQuery): Future[List[PatternQuery]] = {
    val p = promise[List[PatternQuery]]
    val id = maxQueryId.incrementAndGet
    g.addVertex(new QueryVertex(id, p, q.tickets), blocking = true)
    //println(s"Added query vertex for query id $id")
    g.sendSignal(q.withId(id), q.nextTargetId.get, None)
    p.future
  }

  def awaitIdle {
    g.awaitIdle
  }
  
  def shutdown {
    g.shutdown
  }
}