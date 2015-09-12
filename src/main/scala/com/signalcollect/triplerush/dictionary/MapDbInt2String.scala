/*
 *  @author Jahangir Mohammed
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

package com.signalcollect.triplerush.dictionary

import org.mapdb.DBMaker
import org.mapdb.BTreeKeySerializer
import org.mapdb.Serializer
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger

final class MapDbInt2String(val nodeSize: Int = 256) extends Id2String {

  private[this] val utf8 = Charset.forName("UTF-8")
  private[this] val nextId = new AtomicInteger(-1)

  private[this] val db = DBMaker
    .memoryUnsafeDB()
    .closeOnJvmShutdown()
    .transactionDisable()
    .asyncWriteEnable()
    .asyncWriteQueueSize(5000)
    .asyncWriteFlushDelay(5000) // TODO: Evaluate different values
    .compressionEnable() // TODO: Verify this does something useful
    .make()

  private[this] val btree = db.treeMapCreate("btree")
    .keySerializer(BTreeKeySerializer.INTEGER)
    .valueSerializer(Serializer.BYTE_ARRAY)
    .nodeSize(nodeSize) // Default
    .makeOrGet[Int, Array[Byte]]()

  /**
   * Entry IDs have to start with 0 and increase in increments of 1.
   */
  def addEntry(s: String): Int = {
    val encoded = s.getBytes(utf8)
    val id = nextId.incrementAndGet()
    btree.put(id, encoded)
    id
  }

  def contains(i: Int): Boolean = {
    btree.containsKey(i)
  }

  def apply(i: Int): String = {
    val encoded = btree.get(i)
    new String(encoded, utf8)
  }

  def clear(): Unit = {
    btree.clear()
    nextId.set(-1)
  }

  def close(): Unit = {
    btree.clear()
    btree.close()
  }

}
