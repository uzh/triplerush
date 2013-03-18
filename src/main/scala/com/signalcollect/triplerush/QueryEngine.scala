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

class QueryEngine {
  private val g = GraphBuilder.build
  g.setUndeliverableSignalHandler { (signal, id, sourceId, graphEditor) =>
    val queries = signal.asInstanceOf[List[PatternQuery]]
    queries foreach { query =>
      println("failed query: " + query.nextTargetId + " bindings: " + query.bindings)
      graphEditor.sendSignal(query.failed, query.queryId, None)
    }
  }
  g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous))
  g.awaitIdle

  def load(ntriplesFilename: String) {
    val is = new FileInputStream(ntriplesFilename)
    val nxp = new NxParser(is)
    var triplesRead = 0
    println("Reading triples ...")
    while (nxp.hasNext) {
      val triple = nxp.next
      val subjectString = triple(0).toString
      val predicateString = triple(1).toString
      val objectString = triple(2).toString
      g.addVertex(new TripleVertex(
        TriplePattern(
          Mapping.register(subjectString),
          Mapping.register(predicateString),
          Mapping.register(objectString))))
      if (triplesRead % 10000 == 0) {
        println("Triples read: " + triplesRead)
      }
      triplesRead += 1
    }
    print("Waiting for graph loading to finish ... ")
    g.awaitIdle
    println("done")
  }

  private val maxQueryId = new AtomicInteger

  def executeQuery(q: PatternQuery): Future[List[PatternQuery]] = {
    val p = promise[List[PatternQuery]]
    val id = maxQueryId.incrementAndGet
    g.addVertex(new QueryVertex(id, p))
    g.sendSignal(List(q.withId(id)), q.nextTargetId.get, None)
    p.future
  }

  def shutdown {
    g.shutdown
  }
}