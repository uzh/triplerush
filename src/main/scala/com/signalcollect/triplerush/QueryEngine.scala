package com.signalcollect.triplerush

import java.io.FileInputStream
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import org.semanticweb.yars.nx.parser.NxParser
import com.signalcollect._
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.triplerush.Expression.int2Expression

case class QueryEngine() {
  private val g = GraphBuilder.withMessageBusFactory(new BulkAkkaMessageBusFactory(1024, false)).build
  g.setUndeliverableSignalHandler { (signal, id, sourceId, graphEditor) =>
    signal match {
      case query: PatternQuery =>
        graphEditor.sendSignal(query, query.queryId, None)
      case other =>
        println(s"Failed signal delivery $other of type ${other.getClass}")
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
            graphEditor.addVertex(new TripleVertex(tp))
            if (bidirectionalPredicates) {
              val reverseTp = TriplePattern(oId, pId, sId)
              graphEditor.addVertex(new TripleVertex(reverseTp))
            }
          }
        }
      }
      is.close
    }
  }

  def executeQuery(q: PatternQuery): Future[(List[PatternQuery], Map[String, Any])] = {
    val p = promise[(List[PatternQuery], Map[String, Any])]
    if (!q.unmatched.isEmpty) {
      g.addVertex(new QueryVertex(q.queryId, p, q.tickets), blocking = true)
      g.sendSignal(q, q.unmatched.head.routingAddress, None)
      p.future
    } else {
      p success (List(), Map())
      p.future
    }
  }

  def awaitIdle {
    g.awaitIdle
  }

  def shutdown = g.shutdown

}