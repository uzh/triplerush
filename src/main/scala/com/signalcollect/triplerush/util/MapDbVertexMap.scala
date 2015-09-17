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
 */

package com.signalcollect.triplerush.util

import java.util.concurrent.Executors
import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import org.mapdb.{ BTreeKeySerializer, DBMaker }
import org.mapdb.DBMaker.Maker
import org.mapdb.Serializer
import com.signalcollect.Vertex
import com.signalcollect.interfaces.VertexStore
import akka.serialization.Serialization
import com.signalcollect.configuration.ActorSystemRegistry
import akka.actor.ExtendedActorSystem
import com.signalcollect.triplerush.EfficientIndexPattern._

final class MapDbVertexMap(
    val nodeSize: Int = 32,
    dbMaker: Maker = DBMaker
      .memoryUnsafeDB
      .closeOnJvmShutdown
      .transactionDisable
      .asyncWriteEnable
      .asyncWriteQueueSize(32768)
      .storeExecutorEnable(Executors.newScheduledThreadPool(math.min(16, Runtime.getRuntime.availableProcessors))),
    serializer: Serialization = new Serialization(ActorSystemRegistry.systems.values.head.asInstanceOf[ExtendedActorSystem])) extends VertexStore[Long, Any] {

  private[this] val db = dbMaker.make

  private[this] val id2Vertex = db.treeMapCreate("vertex-map")
    .keySerializer(BTreeKeySerializer.LONG)
    .valueSerializer(Serializer.BYTE_ARRAY)
    .nodeSize(nodeSize)
    .makeOrGet[Long, Array[Byte]]()

  private[this] val nonSerializingMap = new TripleRushVertexMap(32, 0.5f)

  @inline private[this] def toVertex(bytes: Array[Byte]): Vertex[Long, _, Long, Any] = {
    serializer.deserialize(bytes, classOf[Vertex[Long, _, Long, Any]]).get
  }

  @inline private[this] def toBytes(vertex: Vertex[Long, _, Long, Any]): Array[Byte] = {
    serializer.serialize(vertex).get
  }

  def get(id: Long): Vertex[Long, _, Long, Any] = {
    val first = id.extractFirst
    val second = id.extractSecond
    if (first < 0 && second < 0) {
      nonSerializingMap.get(id)
    } else {
      val bytes = id2Vertex.get(id)
      if (bytes != null) {
        toVertex(bytes)
      } else {
        null.asInstanceOf[Vertex[Long, _, Long, Any]]
      }
    }
  }

  def put(vertex: Vertex[Long, _, Long, Any]): Boolean = {
    val id = vertex.id
    val first = id.extractFirst
    val second = id.extractSecond
    if (first < 0 && second < 0) {
      nonSerializingMap.put(vertex)
    } else {
      Option(id2Vertex.putIfAbsent(vertex.id, toBytes(vertex))).isEmpty
    }
  }

  def remove(id: Long): Unit = {
    id2Vertex.remove(id)
  }

  def isEmpty: Boolean = {
    id2Vertex.isEmpty && nonSerializingMap.isEmpty
  }

  def size: Long = {
    id2Vertex.size + nonSerializingMap.size
  }

  def stream(): Stream[Vertex[Long, _, Long, Any]] = {
    //id2Vertex.valuesIterator.map(toVertex(_)).toStream
    ???
  }

  def foreach(f: Vertex[Long, _, Long, Any] => Unit): Unit = {
    //id2Vertex.valuesIterator.map(toVertex(_)).foreach(f)
    ???
  }

  def process(p: Vertex[Long, _, Long, Any] => Unit, numberOfVertices: Option[Int] = None): Int = {
    //    var processed = 0
    //    val i = numberOfVertices match {
    //      case None    => id2Vertex.keysIterator
    //      case Some(n) => id2Vertex.keysIterator.take(n)
    //    }
    //    i.foreach { id =>
    //      val vertex = toVertex(id2Vertex.remove(id))
    //      p(vertex)
    //      processed += 1
    //    }
    //    processed
    ???
  }

  def processWithCondition(p: Vertex[Long, _, Long, Any] => Unit, breakCondition: () => Boolean): Int = {
    //    var processed = 0
    //    val i = id2Vertex.keysIterator
    //    while (breakCondition() == false && i.hasNext) {
    //      val id = i.next
    //      val vertex = toVertex(id2Vertex.remove(id))
    //      p(vertex)
    //      processed += 1
    //    }
    //    processed
    ???
  }

}
