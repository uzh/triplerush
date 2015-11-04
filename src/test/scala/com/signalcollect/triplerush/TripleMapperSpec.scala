package com.signalcollect.triplerush

import org.scalatest.prop.Checkers
import org.scalacheck.Prop._
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.signalcollect.triplerush.mapper.DistributedTripleMapper
import com.signalcollect.triplerush.mapper.RelievedNodeZeroTripleMapper

class TripleMapperSpec extends FlatSpec with Matchers with Checkers {

  val numberOfNodes = 8
  val workersPerNode = 24
  val step = numberOfNodes * workersPerNode

  val distributedMapper = new DistributedTripleMapper(numberOfNodes = numberOfNodes, workersPerNode = workersPerNode)
  val relievedNodeZeroMapper = new RelievedNodeZeroTripleMapper(numberOfNodes = numberOfNodes, workersPerNode = workersPerNode)

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
        val node1 = nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(subjectId, 2, 0)))
        val node2 = nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(subjectId, 0, 5)))
        node1 == node2
      }
    }, minSuccessful(10))
  }

  it should "assign triples that share subject/object to the same node, if the one with the object has no subject set" in {
    check((id: Int) => {
      (id > 0) ==> {
        val node1 = nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(id, 2, 0)))
        val node2 = nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(0, 5, id)))
        node1 == node2
      }
    }, minSuccessful(100))
  }

  it should "usually assign triples that share subject/object to different workers on the same node, when their predicates are different" in {
    check((id: Int) => {
      (id > 0) ==> {
        val w1 = workerId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(id, 2, 0)))
        val w2 = workerId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(0, 5, id)))
        w1 != w2
      }
    }, minSuccessful(100))
  }

  /**
   * Usually small ids are more frequent. Try to avoid putting them all
   * on the same node.
   */
  it should "not assign all the small ids to the same node" in {
    val node1 = nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(1, 2, 0)))
    val node2 = nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(2, 5, 0)))
    val node3 = nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(3, 5, 0)))
    node1 != node2
    node2 != node3
    node1 != node3
  }

  it should "always assign queries to node 0" in {
    check((queryVertexId: Int) => {
      if (queryVertexId != 0 && queryVertexId != Int.MinValue) {
        val queryWorkerId = distributedMapper.getWorkerIdForVertexId(OperationIds.embedInLong(queryVertexId))
        val node = nodeId(queryWorkerId)
        node == 0
      } else {
        true
      }
    }, minSuccessful(10))
  }

  "RelievedNodeZeroMapper" should "not assign (non-root) triple patterns to node 0" in {
    assert(nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(1, 2, 0))) != 0)
    assert(nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(0, 7, 5))) != 0)
    assert(nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(9, 0, 6))) != 0)
    assert(nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(7, 0, 0))) != 0)
    assert(nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(0, 3, 0))) != 0)
    assert(nodeId(distributedMapper.getWorkerIdForVertexId(EfficientIndexPattern(0, 0, 2))) != 0)
  }

  it should "always assign queries to node 0" in {
    check((queryVertexId: Int) => {
      if (queryVertexId != 0 && queryVertexId != Int.MinValue) {
        val queryWorkerId = relievedNodeZeroMapper.getWorkerIdForVertexId(OperationIds.embedInLong(queryVertexId))
        val node = nodeId(queryWorkerId)
        node == 0
      } else {
        true
      }
    }, minSuccessful(10))
  }

}
