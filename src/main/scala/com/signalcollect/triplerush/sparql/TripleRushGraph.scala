/*
 *  @author Philip Stutz
 *
 *  Copyright 2015 iHealth Technologies
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
 *
 */

package com.signalcollect.triplerush.sparql

import scala.collection.JavaConversions.asJavaIterator
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.apache.jena.graph.{GraphEvents, GraphStatisticsHandler, Node, Node_ANY, Node_Literal, Node_URI, Triple}
import org.apache.jena.graph.impl.GraphBase
import org.apache.jena.query.ARQ
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.engine.main.StageGenerator
import org.apache.jena.util.iterator.{ExtendedIterator, WrappedIterator}
import com.signalcollect.triplerush.{TriplePattern, TripleRush}
import org.apache.jena.graph.Node_Blank
import org.apache.jena.graph.Node_Variable
import org.apache.jena.graph.Capabilities

/**
 * A TripleRush implementation of the Jena Graph interface.
 */
class TripleRushGraph(val tr: TripleRush = new TripleRush) extends GraphBase with GraphStatisticsHandler {

  def getModel = ModelFactory.createModelForGraph(this)

  // Set TripleRushStageGenerator as default for all queries.
  val tripleRushStageGen = ARQ.getContext.get(ARQ.stageGenerator) match {
    case g: TripleRushStageGenerator =>
      new TripleRushStageGenerator(g.other)
    case otherGraph: StageGenerator =>
      new TripleRushStageGenerator(otherGraph)
    case _: Any => throw new Exception("No valid stage generator found.")
  }
  ARQ.getContext.set(ARQ.stageGenerator, tripleRushStageGen)

  def getStatistic(s: Node, p: Node, o: Node): Long = {
    val q = Seq(arqNodesToPattern(s, p, o))
    val countOptionFuture = tr.executeCountingQuery(q)
    val countOption = Await.result(countOptionFuture, 300.seconds)
    val count = countOption.getOrElse(throw new Exception(s"Incomplete counting query execution for $q."))
    count
  }

  override def createStatisticsHandler = this

  /**
   * Meaning of prefixes in the encoded string:
   * - Everything that starts with a letter is interpreted as an IRI,
   * because their schema has to start with a letter.
   * - If a string starts with a digit or a hyphen, then it is interpreted as an integer literal.
   * - If a string starts with `"` or "<", then it is interpreted as a general literal.
   */
  override def performAdd(triple: Triple): Unit = {
    tr.addTriple(triple, blocking = true)
  }

  override def clear: Unit = {
    getEventManager.notifyEvent(this, GraphEvents.removeAll)
    tr.clear
  }

  override def close: Unit = {
    super.close
    tr.shutdown
  }

  def graphBaseFind(triplePattern: Triple): ExtendedIterator[Triple] = {
    val s = triplePattern.getSubject
    val p = triplePattern.getPredicate
    val o = triplePattern.getObject
    val pattern = arqNodesToPattern(s, p, o)
    val resultIterator = tr.resultIteratorForQuery(Seq(pattern))
    val concreteS = if (s.isConcrete) Some(NodeConversion.nodeToString(s)) else None
    val concreteP = if (p.isConcrete) Some(NodeConversion.nodeToString(p)) else None
    val concreteO = if (o.isConcrete) Some(NodeConversion.nodeToString(o)) else None
    val convertedIterator = TripleRushIterator.convert(concreteS, concreteP, concreteO, tr.dictionary, resultIterator)
    WrappedIterator.createNoRemove(convertedIterator)
  }

  override def graphBaseContains(t: Triple): Boolean = {
    getStatistic(t.getSubject, t.getPredicate, t.getObject) >= 1
  }

  override def graphBaseSize: Int = {
    val wildcard = Node.ANY
    val sizeAsLong = getStatistic(wildcard, wildcard, wildcard)
    if (sizeAsLong <= Int.MaxValue) {
      sizeAsLong.toInt
    } else {
      Int.MaxValue // Better than crashing?
    }
  }

  // TODO: Make more efficient by unrolling everything.
  // TODO: Does not support using the same variable/blank node multiple times. Test if this case needs to be supported.
  private[this] def arqNodesToPattern(s: Node, p: Node, o: Node): TriplePattern = {
    var nextVariableId = -1
    @inline def nodeToId(n: Node): Int = {
      n match {
        case variable: Node_ANY =>
          val id = nextVariableId
          nextVariableId -= 1
          id
        case variable: Node_Variable =>
          throw new UnsupportedOperationException("Variables not supported.")
        case blank: Node_Blank =>
          tr.dictionary(NodeConversion.nodeToString(blank))
        case other: Node =>
          tr.dictionary(NodeConversion.nodeToString(other))
      }
    }
    val sId = nodeToId(s)
    val pId = nodeToId(p)
    val oId = nodeToId(o)
    TriplePattern(sId, pId, oId)
  }

  override val getCapabilities = new Capabilities {
    val sizeAccurate = true
    val addAllowed = true

    def addAllowed(everyTriple: Boolean) = true

    val deleteAllowed = false

    def deleteAllowed(everyTriple: Boolean) = false

    val iteratorRemoveAllowed = false
    val canBeEmpty = true
    val findContractSafe = true
    val handlesLiteralTyping = false
  }

}
