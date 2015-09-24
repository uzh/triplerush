package com.signalcollect.triplerush.sparql

import org.apache.jena.graph.Graph
import org.apache.jena.graph.test.AbstractTestGraph
import com.signalcollect.triplerush.{TestConfig, TripleRush}
import org.junit.runner.RunWith
import org.junit.runners.Suite
import com.signalcollect.GraphBuilder
import akka.actor.ActorSystem
import java.util.UUID
import org.apache.jena.graph.test.GraphTestBase
import org.apache.jena.test.JenaTestBase
import org.apache.jena.rdf.model.ModelFactory
import java.io.InputStream

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(classOf[TripleRushGraphTest]))
class GraphTestSuite

class TripleRushGraphTest(name: String) extends AbstractTestGraph(name) {

  def getGraph: Graph = {
    val tr = getTripleRushInstance
    tr.prepareExecution
    TripleRushGraph(tr)
  }

  private[this] def getTripleRushInstance: TripleRush = {
    val trSystem = ActorSystem(UUID.randomUUID.toString)
    val config = TestConfig.system()
    TripleRush(graphBuilder = new GraphBuilder[Long, Any]().withActorSystem(trSystem), config = config)
  }

  override def getGraphWith(facts: String): Graph = {
    val tr = getTripleRushInstance
    try {
      val g = TripleRushGraph(tr)
      GraphTestBase.graphAdd(g, facts)
      tr.prepareExecution
      g
    } finally {
      tr.shutdown()
      tr.system.shutdown()
    }
  }

  override def testIsomorphismFile(): Unit = {
    testIsomorphismXMLFile(1, true)
    testIsomorphismXMLFile(2, true)
    testIsomorphismXMLFile(3, true)
    testIsomorphismXMLFile(4, true)
    testIsomorphismXMLFile(5, false)
    testIsomorphismXMLFile(6, false)
    testIsomorphismNTripleFile(7, true)
    testIsomorphismNTripleFile(8, false)
  }

  def testIsomorphismNTripleFile(i: Int, result: Boolean): Unit = {
    testIsomorphismFile(i, "N-TRIPLE", "nt", result)
  }

  def testIsomorphismXMLFile(i: Int, result: Boolean): Unit = {
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

  override def testRemove(): Unit = {}

  override def testBulkDeleteList(): Unit = {}

  override def testBulkDeleteArray(): Unit = {}

  override def testBulkDeleteGraph(): Unit = {}

  override def testEventDeleteByFind(): Unit = {}

  override def testBulkUpdate(): Unit = {}

  override def testBulkDeleteIterator(): Unit = {}

  override def testDeleteTriple(): Unit = {}

  override def testRemoveAll(): Unit = {}

  override def testRemoveSPO(): Unit = {}

  override def testListSubjects(): Unit = {}

  // Uses GraphBase::delete.
  override def testListPredicates(): Unit = {}

  // Uses GraphBase::delete.
  override def testListObjects(): Unit = {}

  // Uses GraphBase::delete.
  override def testUnregisterOnce(): Unit = {}

  // Uses GraphBase::delete.
  override def testIsEmpty(): Unit = {}

  // Uses GraphBase::delete.
  override def testAGraph(): Unit = {} // Uses GraphBase::delete.

}
