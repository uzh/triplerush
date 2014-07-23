package com.signalcollect.triplerush

import org.scalatest.prop.Checkers
import org.scalacheck.Prop._
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class TripleMapperSpec extends FlatSpec with Matchers with Checkers with TestAnnouncements {

  val numberOfNodes = 8
  val workersPerNode = 24
  val step = numberOfNodes * workersPerNode

  val m = new TripleMapper(numberOfNodes = numberOfNodes, workersPerNode = workersPerNode)

  def nodeId(workerId: Int) = (((workerId & Int.MaxValue) % step) / workersPerNode).floor.toInt
  def workerId(workerId: Int) = (workerId & Int.MaxValue) % workersPerNode

  assert(nodeId(0) == 0)
  assert(nodeId(1) == 0)
  assert(nodeId(3) == 0)
  assert(nodeId(24) == 1)
  assert(nodeId(191) == 7)
  assert(nodeId(25) == 1)
  assert(workerId(25) == 1)
  assert(workerId(1) == 1)
  assert(workerId(191) == 23)

  "TripleMapper" should "assign triples with the same subject to the same node" in {
    check((subjectId: Int) => {
      (subjectId > 0) ==> {
        val node1 = nodeId(m.getWorkerIdForVertexId(TriplePattern(subjectId, 2, 0).toEfficientIndexPattern))
        val node2 = nodeId(m.getWorkerIdForVertexId(TriplePattern(subjectId, 0, 5).toEfficientIndexPattern))
        node1 == node2
      }
    }, minSuccessful(100))
  }

  it should "assign triples that share subject/object to the same node, if the one with the object has no subject set" in {
    check((id: Int) => {
      (id > 0) ==> {
        val node1 = nodeId(m.getWorkerIdForVertexId(TriplePattern(id, 2, 0).toEfficientIndexPattern))
        val node2 = nodeId(m.getWorkerIdForVertexId(TriplePattern(0, 5, id).toEfficientIndexPattern))
        node1 == node2
      }
    }, minSuccessful(100))
  }

  it should "usually assign triples that share subject/object to different workers on the same node, if their predicates are different" in {
    check((id: Int) => {
      (id > 0) ==> {
        val w1 = workerId(m.getWorkerIdForVertexId(TriplePattern(id, 2, 0).toEfficientIndexPattern))
        val w2 = workerId(m.getWorkerIdForVertexId(TriplePattern(0, 5, id).toEfficientIndexPattern))
        w1 != w2
      }
    }, minSuccessful(100))
  }

  /**
   * Usually small ids are more frequent. Try to avoid putting them all
   * on the same node.
   */
  it should "not assign all the small ids to the same node" in {
    val node1 = nodeId(m.getWorkerIdForVertexId(TriplePattern(1, 2, 0).toEfficientIndexPattern))
    val node2 = nodeId(m.getWorkerIdForVertexId(TriplePattern(2, 5, 0).toEfficientIndexPattern))
    val node3 = nodeId(m.getWorkerIdForVertexId(TriplePattern(3, 5, 0).toEfficientIndexPattern))
    node1 != node2
    node2 != node3
    node1 != node3
  }

  it should "always assign queries to node 0" in {
    check((queryVertexId: Int) => {
      if (queryVertexId != 0) {
        val queryWorkerId = m.getWorkerIdForVertexId(queryVertexId)
        val node = nodeId(queryWorkerId)
        node == 0
      } else {
        true
      }
    }, minSuccessful(1000))
  }

}
