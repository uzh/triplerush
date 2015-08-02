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
import com.hp.hpl.jena.rdf.model.ModelFactory
import java.io.InputStream

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(classOf[TripleRushGraphTest]))
class GraphTestSuite

class TripleRushGraphTest(name: String) extends AbstractTestGraph(name) {

  def getGraph: Graph = {
    val tr = getTripleRushInstance
    tr.prepareExecution
    new TripleRushGraph(tr)
  }

  private[this] def getTripleRushInstance: TripleRush = {
    val trSystem = ActorSystem(UUID.randomUUID.toString)
    new TripleRush(graphBuilder = new GraphBuilder[Long, Any]().withActorSystem(trSystem))
  }

  override def getGraphWith(facts: String): Graph = {
    val tr = getTripleRushInstance
    val g = new TripleRushGraph(tr)
    GraphTestBase.graphAdd(g, facts)
    tr.prepareExecution
    g
  }

  override def testIsomorphismFile() {
    testIsomorphismXMLFile(1, true)
    testIsomorphismXMLFile(2, true)
    testIsomorphismXMLFile(3, true)
    testIsomorphismXMLFile(4, true)
    testIsomorphismXMLFile(5, false)
    testIsomorphismXMLFile(6, false)
    testIsomorphismNTripleFile(7, true)
    testIsomorphismNTripleFile(8, false)
  }

  def testIsomorphismNTripleFile(i: Int, result: Boolean) {
    testIsomorphismFile(i, "N-TRIPLE", "nt", result)
  }

  def testIsomorphismXMLFile(i: Int, result: Boolean) {
    testIsomorphismFile(i, "RDF/XML", "rdf", result)
  }

  def testIsomorphismFile(n: Int, lang: String, suffix: String, result: Boolean): Unit = {
    println("OVERRIDDEN")
    val g1 = getGraph
    val g2 = getGraph
    val m1 = ModelFactory.createModelForGraph(g1)
    val m2 = ModelFactory.createModelForGraph(g2)
    m1.read(
      getInputStream(n, 1, suffix),
      "http://www.example.org/", lang)
    m2.read(
      getInputStream(n, 2, suffix),
      "http://www.example.org/", lang)
    val rslt = g1.isIsomorphicWith(g2) == result
    if (!rslt) {
      System.out.println("g1:")
      m1.write(System.out, "N-TRIPLE")
      System.out.println("g2:")
      m2.write(System.out, "N-TRIPLE")
    }
    assert(rslt, "Isomorphism test failed")
  }

  def getInputStream(n: Int, n2: Int, suffix: String): InputStream = {
    val urlStr = s"regression/testModelEquals/$n-$n2.$suffix"
    classOf[AbstractTestGraph].getClassLoader.getResourceAsStream(urlStr)
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
  override def testListObjects() {} // Uses GraphBase::delete.
  override def testUnregisterOnce() {} // Uses GraphBase::delete.
  override def testIsEmpty() {} // Uses GraphBase::delete.
  override def testAGraph() {} // Uses GraphBase::delete.

}
