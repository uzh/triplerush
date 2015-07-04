package com.signalcollect.triplerush.sparql

import com.hp.hpl.jena.graph.Graph
import com.hp.hpl.jena.graph.test.AbstractTestGraph
import com.signalcollect.triplerush.TripleRush
import org.junit.runner.RunWith
import org.junit.runners.Suite
import com.signalcollect.GraphBuilder
import akka.actor.ActorSystem
import java.util.UUID
import com.hp.hpl.jena.graph.test.GraphTestBase
import com.hp.hpl.jena.test.JenaTestBase

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

  override def testContainsConcrete() {
    import GraphTestBase.triple
    val g = getGraphWith("s P o; _x _R _y; x S 0")
    assert(g.contains(triple("s P o")) == true)
    assert(g.contains(triple("_x _R _y")) == true)
    assert(g.contains(triple("x S 0")) == true)

    assert(g.contains(triple("s P Oh")) == false)
    assert(g.contains(triple("S P O")) == false)
    assert(g.contains(triple("s p o")) == false)
    assert(g.contains(triple("_x _r _y")) == false)
    assert(g.contains(triple("x S 1")) == false)
  }

  override def testRemove() {}
  override def testBulkDeleteList() {}
  override def testBulkDeleteArray() {}
  override def testBulkDeleteGraph() {}
  override def testEventDeleteByFind() {}
  override def testBulkUpdate() {}
  override def testBulkDeleteIterator() {}
  override def testDeleteTriple() {}
  override def testRemoveAll() {}
  override def testRemoveSPO() {}
  override def testListSubjects() {} // Uses GraphBase::delete.
  override def testListPredicates() {} // Uses GraphBase::delete.
  override def testUnregisterOnce() {} // Uses GraphBase::delete.
  override def testIsEmpty() {} // Uses GraphBase::delete.
  override def testAGraph() {} // Uses GraphBase::delete.

}
