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
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.GraphEditor
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.StateForwarderEdge

case class QueryEngine() {
  private val g = GraphBuilder.withMessageBusFactory(new BulkAkkaMessageBusFactory(196, false)).build
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

  def load(ntriplesFilename: String,
    bidirectionalPredicates: Boolean = false,
    onlyTriplesAboutKnownEntities: Boolean = false,
    bannedPredicates: Set[String] = Set()) {
    g.modifyGraph(loadFile _, None)
    def loadFile(graphEditor: GraphEditor[Any, Any]) {
      val is = new FileInputStream(ntriplesFilename)
      val nxp = new NxParser(is)
      //    var triplesRead = 0
      println(s"Reading triples from $ntriplesFilename ...")
      while (nxp.hasNext) {
        val triple = nxp.next
        val predicateString = triple(1).toString
        if (!bannedPredicates.contains(predicateString)) {
          val subjectString = triple(0).toString
          val objectString = triple(2).toString
          if (!onlyTriplesAboutKnownEntities || Mapping.existsMappingForString(subjectString) || Mapping.existsMappingForString(objectString)) {
            val sId = Mapping.register(subjectString)
            val pId = Mapping.register(predicateString)
            val oId = Mapping.register(objectString)
            val tp = TriplePattern(sId, pId, oId)
            //            QueryOptimizer.addTriple(tp)
            graphEditor.addVertex(new IndexVertex(tp))
            if (bidirectionalPredicates) {
              val reverseTp = TriplePattern(oId, pId, sId)
              //              QueryOptimizer.addTriple(reverseTp)
              graphEditor.addVertex(new IndexVertex(reverseTp))
              //            triplesRead += 1
            }
            //          triplesRead += 1
            //          if (triplesRead % 1000 == 0) {
            //            println("Triples read: " + triplesRead)
            //          }

          }
        }
      }
    }
    //    print("Waiting for graph loading to finish ... ")
  }

  def executeQuery(q: PatternQuery): Future[ArrayBuffer[PatternQuery]] = {
    val p = promise[ArrayBuffer[PatternQuery]]
    if (!q.unmatched.isEmpty) {
      g.addVertex(new QueryVertex(q.queryId, p, q.tickets), blocking = true)
      g.sendSignal(q, q.unmatched.head.routingAddress, None)
      p.future
    } else {
      p success ArrayBuffer()
      p.future
    }
  }

  def awaitIdle {
    g.awaitIdle
  }

  def shutdown = g.shutdown

}