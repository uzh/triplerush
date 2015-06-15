package com.signalcollect.triplerush.sparql

import com.hp.hpl.jena.graph.Graph
import com.hp.hpl.jena.graph.test.AbstractTestGraph
import com.signalcollect.triplerush.TripleRush
import org.junit.runner.RunWith
import org.junit.runners.Suite
import com.signalcollect.GraphBuilder
import akka.actor.ActorSystem
import java.util.UUID

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(classOf[TripleRushGraphTest]))
class GraphTestSuite

class TripleRushGraphTest(name: String) extends AbstractTestGraph(name) {
  
  def getGraph: Graph = {
    val trSystem = ActorSystem(UUID.randomUUID.toString)
    val tr = new TripleRush(
      graphBuilder = new GraphBuilder[Long, Any]().withActorSystem(trSystem))
    tr.prepareExecution
    new TripleRushGraph(tr)
  }

}
