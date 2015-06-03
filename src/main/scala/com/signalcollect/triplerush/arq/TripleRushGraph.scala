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

package com.signalcollect.triplerush.arq

import scala.collection.JavaConversions.asJavaIterator
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import com.hp.hpl.jena.graph._
import com.hp.hpl.jena.graph.impl.GraphBase
import com.hp.hpl.jena.util.iterator.{ ExtendedIterator, WrappedIterator }
import com.signalcollect.triplerush.{ Dictionary, TriplePattern, TripleRush }

/**
 * A TripleRush implementation of the Jena Graph interface.
 */
class TripleRushGraph(tr: TripleRush) extends GraphBase with GraphStatisticsHandler {

  def getStatistic(s: Node, p: Node, o: Node): Long = {
    val q = Seq(arqNodesToPattern(s, p, o))
    val countOptionFuture = tr.executeCountingQuery(q)
    val countOption = Await.result(countOptionFuture, 300.seconds)
    val count = countOption.getOrElse(throw new Exception(s"Incomplete counting query execution for $q."))
    count
  }

  override def createStatisticsHandler = this

  override def performAdd(triple: Triple): Unit = {
    val s = triple.getSubject.toString
    val p = triple.getPredicate.toString
    val o = triple.getObject.toString
    tr.addTriple(s, p, o)
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
    val pattern = TripleToPattern(triplePattern)
    val resultIterator = tr.resultIteratorForQuery(Seq(pattern))
    val sOption = nodeToString(triplePattern.getSubject)
    val pOption = nodeToString(triplePattern.getPredicate)
    val oOption = nodeToString(triplePattern.getObject)
    val convertedIterator = TripleRushIterator.convert(sOption, pOption, oOption, tr.dictionary, resultIterator)
    WrappedIterator.createNoRemove(convertedIterator)
  }

  override def graphBaseContains(t: Triple): Boolean = {
    getStatistic(t.getSubject, t.getPredicate, t.getObject) >= 1
  }

  override def graphBaseSize: Int = {
    val s = NodeFactory.createVariable("s")
    val p = NodeFactory.createVariable("p")
    val o = NodeFactory.createVariable("o")
    val sizeAsLong = getStatistic(s, p, o)
    if (sizeAsLong <= Int.MaxValue) {
      sizeAsLong.toInt
    } else {
      Int.MaxValue // Better than crashing?
    }
  }

  private def nodeToString(n: Node): Option[String] = {
    if (n.isURI || n.isLiteral) {
      Some(n.toString)
    } else if (n.isVariable) {
      None
    } else {
      throw new UnsupportedOperationException(s"TripleRush does not support node $n of type ${n.getClass.getSimpleName}.")
    }
  }

  private def TripleToPattern(triple: Triple): TriplePattern = {
    arqNodesToPattern(triple.getSubject, triple.getPredicate, triple.getMatchObject)
  }

  // TODO: Make more efficient by unrolling everything and getting rid of the map.
  private def arqNodesToPattern(s: Node, p: Node, o: Node): TriplePattern = {
    var nextVariableId = -1
    var variableMap = Map.empty[String, Int]
    @inline def nodeToId(n: Node): Int = {
      n match {
        case variable: Node_Variable =>
          val name = variable.getName
          if (variableMap.contains(name)) {
            // Reuse ID.
            variableMap(name)
          } else {
            val id = nextVariableId
            nextVariableId -= 1
            variableMap += ((name, id))
            id
          }
        case uri: Node_URI =>
          tr.dictionary(uri.toString(null, false))
        case literal: Node_Literal =>
          tr.dictionary(literal.toString(null, false))
        case other =>
          throw new UnsupportedOperationException(s"TripleRush does not yet support node $other of type ${other.getClass.getSimpleName}.")
      }
    }
    val sId = nodeToId(s)
    val pId = nodeToId(p)
    val oId = nodeToId(o)
    TriplePattern(sId, pId, oId)
  }

}
