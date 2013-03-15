package com.signalcollect.triplerush

import org.semanticweb.yars.nx.parser.NxParser
import java.io.FileInputStream
import com.signalcollect.GraphBuilder
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.configuration.ExecutionMode
import scala.concurrent.{ future, promise }
import scala.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger

class QueryEngine {
  private val g = GraphBuilder.build
  // TODO Undeliverable signal handler has to notify the corresponding QueryVertex about the missing fraction. 
  g.setUndeliverableSignalHandler { (signal, id, sourceId, graphEditor) =>
    val queries = signal.asInstanceOf[List[PatternQuery]]
    queries foreach { query =>
      println("failed query: " + query.nextTargetId + " bindings: " + query.bindings)
      graphEditor.sendSignal(query.failed, query.queryId, None)
    }
  }
  //println("Undeliverable: targetId=" + id + " signal=" + s)
  g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous))
  g.awaitIdle

  def load(ntriplesFilename: String) {
    val is = new FileInputStream(ntriplesFilename)
    val nxp = new NxParser(is)
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
    }
    g.awaitIdle
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