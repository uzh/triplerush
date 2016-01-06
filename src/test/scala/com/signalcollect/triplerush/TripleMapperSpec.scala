/*
 *  @author Philip Stutz
 *
 *  Copyright 2014 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.signalcollect.triplerush

import org.scalacheck.{ Arbitrary, Gen, Prop }
import org.scalacheck.Prop.propBoolean
import org.scalatest.{ Finders, FlatSpec, Matchers }
import org.scalatest.prop.Checkers

import com.signalcollect.triplerush.mapper.{ DistributedTripleMapper, RelievedNodeZeroTripleMapper }

class TripleMapperSpec extends FlatSpec with Matchers with Checkers {

  case class NodeSetup(numberOfNodes: Int, workersPerNode: Int) {
    private[this] val step = numberOfNodes * workersPerNode
    assert(numberOfNodes > 0)
    assert(workersPerNode > 0)
    def nodeId(workerId: Int) = (((workerId & Int.MaxValue) % step) / workersPerNode).floor.toInt
    def nodeRelativeWorkerId(workerId: Int) = (workerId & Int.MaxValue) % workersPerNode
  }

  val genPositiveInt = Gen.choose(1, 100)

  val genZeroOrPositive = Gen.oneOf(Gen.const(0), genPositiveInt)

  val genIndexPattern = {
    val genPattern = for {
      s <- genZeroOrPositive
      p <- genZeroOrPositive
      o <- genZeroOrPositive
    } yield TriplePattern(s, p, o)
    genPattern.filter(!_.isFullyBound)
  }

  implicit val arbIndexPattern = Arbitrary(genIndexPattern)

  val genNodeSetup = {
    for {
      nodes <- Gen.choose(1, 13)
      workersPerNode <- Gen.choose(1, 17)
    } yield NodeSetup(nodes, workersPerNode)
  }

  implicit val arbNodeSetup = Arbitrary(genNodeSetup)

  def distributedMapper(s: NodeSetup) = new DistributedTripleMapper(numberOfNodes = s.numberOfNodes, workersPerNode = s.workersPerNode)
  def relievedNodeZeroMapper(s: NodeSetup) = new RelievedNodeZeroTripleMapper(numberOfNodes = s.numberOfNodes, workersPerNode = s.workersPerNode)

  "TripleMapper" should "assign triples with the same subject to the same node" in {
    check {
      Prop.forAll(genPositiveInt, genNodeSetup) {
        (subjectId: Int, s: NodeSetup) =>
          val m = distributedMapper(s)
          val node1 = s.nodeId(m.getWorkerIdForVertexId(EfficientIndexPattern(subjectId, 2, 0)))
          val node2 = s.nodeId(m.getWorkerIdForVertexId(EfficientIndexPattern(subjectId, 0, 5)))
          assert(node1 == node2)
          true
      }
    }
  }

  it should "assign triples that share subject/object to the same node, if the one with the object has no subject set" in {
    check {
      Prop.forAll(genPositiveInt, genNodeSetup) {
        (positiveId: Int, s: NodeSetup) =>
          val m = distributedMapper(s)
          val node1 = s.nodeId(m.getWorkerIdForVertexId(EfficientIndexPattern(positiveId, 2, 0)))
          val node2 = s.nodeId(m.getWorkerIdForVertexId(EfficientIndexPattern(0, 5, positiveId)))
          assert(node1 == node2)
          true
      }
    }
  }

  it should "assign triples that share subject/object to different workers on the same node, when their predicates are different" in {
    check {
      Prop.forAll(genPositiveInt, genNodeSetup) {
        (positiveId: Int, s: NodeSetup) =>
          val m = distributedMapper(s)
          val p1 = 2
          val p2 = 97
          val pDelta = p2 - p1
          val workerId1 = m.getWorkerIdForVertexId(EfficientIndexPattern(positiveId, p1, 0))
          val workerId2 = m.getWorkerIdForVertexId(EfficientIndexPattern(0, p2, positiveId))
          val relativeWorkerId1 = s.nodeRelativeWorkerId(workerId1)
          val relativeWorkerId2 = s.nodeRelativeWorkerId(workerId2)
          assert(s.nodeId(workerId1) == s.nodeId(workerId2))
          // Only check if there are > 1 workers and if there delta between the IDs is not a multiple of the number of workers
          if (s.workersPerNode > 1 && pDelta % s.workersPerNode != 0) {
            assert(relativeWorkerId1 != relativeWorkerId2)
          }
          true
      }
    }
  }

  /**
   * Usually small ids are more frequent. Try to avoid putting them all
   * on the same node.
   */
  it should "not assign all the small ids to the same node" in {
    check {
      Prop.forAll(genPositiveInt, genNodeSetup) {
        (positiveId: Int, s: NodeSetup) =>
          val m = distributedMapper(s)
          val node1 = s.nodeId(m.getWorkerIdForVertexId(EfficientIndexPattern(1, 2, 0)))
          val node2 = s.nodeId(m.getWorkerIdForVertexId(EfficientIndexPattern(2, 5, 0)))
          val node3 = s.nodeId(m.getWorkerIdForVertexId(EfficientIndexPattern(3, 5, 0)))
          if (s.numberOfNodes >= 3) {
            assert(node1 != node2)
            assert(node2 != node3)
            assert(node1 != node3)
          }
          true
      }
    }
  }

  it should "always assign queries to node 0" in {
    check((queryVertexId: Int, s: NodeSetup) => {
      if (queryVertexId != 0 && queryVertexId != Int.MinValue) {
        val m = distributedMapper(s)
        val queryWorkerId = m.getWorkerIdForVertexId(OperationIds.embedInLong(queryVertexId))
        val node = s.nodeId(queryWorkerId)
        assert(node == 0)
      }
      true
    }, minSuccessful(1000))
  }

  "RelievedNodeZeroMapper" should "not assign (non-root) triple patterns to node 0" in {
    check((tp: TriplePattern, s: NodeSetup) => {
      assert(!tp.isFullyBound, "Index patterns cannot be fully bound.")
      if (s.numberOfNodes > 1) {
        val m = relievedNodeZeroMapper(s)
        val worker = m.getWorkerIdForVertexId(tp.toEfficientIndexPattern)
        val node = s.nodeId(worker)
        if (tp != TriplePattern(0, 0, 0)) {
          assert(node != 0)
        }
      }
      true
    }, minSuccessful(1000))
  }

  it should "always assign queries to node 0" in {
    check((queryVertexId: Int, s: NodeSetup) => {
      if (s.numberOfNodes > 1 && queryVertexId != 0 && queryVertexId != Int.MinValue) {
        val m = relievedNodeZeroMapper(s)
        val queryWorkerId = m.getWorkerIdForVertexId(OperationIds.embedInLong(queryVertexId))
        val node = s.nodeId(queryWorkerId)
        assert(node == 0)
      }
      true
    }, minSuccessful(1000))
  }

}
