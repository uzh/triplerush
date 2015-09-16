/*
 *  @author Philip Stutz
 *  @author Jahangir Mohammed
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

import java.nio.charset.Charset
import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger
import scala.util.hashing.MurmurHash3
import org.mapdb.{ BTreeKeySerializer, DBMaker }
import org.mapdb.DBMaker.Maker
import org.mapdb.Serializer
import scala.annotation.tailrec
import java.util.concurrent.Executors

final class HashDictionary(
    val id2StringNodeSize: Int = 32,
    val string2IdNodeSize: Int = 32,
    dbMaker: Maker = DBMaker
      .memoryUnsafeDB
      .closeOnJvmShutdown
      .transactionDisable
      .asyncWriteEnable
      .asyncWriteQueueSize(4096)
      .storeExecutorEnable(Executors.newScheduledThreadPool(math.min(16, Runtime.getRuntime.availableProcessors)))
//      .metricsEnable(10000)
//      .metricsExecutorEnable
      .compressionEnable) extends RdfDictionary {

  private[this] val db = dbMaker.make

  private[this] val id2String = db.treeMapCreate("int2String")
    .keySerializer(BTreeKeySerializer.INTEGER)
    .valueSerializer(Serializer.BYTE_ARRAY)
    .nodeSize(id2StringNodeSize)
    .makeOrGet[Int, Array[Byte]]()

  //  private[this] val id2String = db.hashMapCreate("int2String")
  //    .keySerializer(Serializer.INTEGER_PACKED)
  //    .valueSerializer(Serializer.BYTE_ARRAY)
  //    .makeOrGet[Int, Array[Byte]]()

  private[this] val string2Id = db.treeMapCreate("string2Int")
    .keySerializer(BTreeKeySerializer.BYTE_ARRAY)
    .valueSerializer(Serializer.INTEGER_PACKED)
    .nodeSize(string2IdNodeSize)
    .makeOrGet[Array[Byte], Int]()

  private[this] val utf8 = Charset.forName("UTF-8")

  def initialize(): Unit = {
    id2String.put(0, "*".getBytes(utf8))
  }

  // idCandidate is the hashCode of the byte array
  private[this] def addEntry(s: Array[Byte], idCandidate: Int): Int = {
    val existing = id2String.putIfAbsent(idCandidate, s)
    if (existing == null) {
      //      println(s"Added string ${new String(s, utf8)} with hash-based ID $idCandidate")
      //      assert(id2String.containsKey(idCandidate)) // TODO: Verify MapDB beta 7 fixes this.
      idCandidate
    } else {
      if (Arrays.equals(s, existing)) {
        //        println(s"Did not add string ${new String(s, utf8)}, because it already exists in the dictionary with ID $idCandidate")
        idCandidate // existing
      } else {
        val id = addEntryToExceptions(s) // collision
        //        println(s"Collision of new string ${new String(s, utf8)} with existing string ${new String(existing, utf8)}, added it with exception ID $id")
        id
      }
    }
  }

  val allIdsTakenUpTo = new AtomicInteger(0)

  def addEntryToExceptions(s: Array[Byte]): Int = {
    @tailrec def recursiveAddEntryToExceptions(s: Array[Byte]): Int = {
      val attemptedId = allIdsTakenUpTo.incrementAndGet
      val existing = id2String.putIfAbsent(attemptedId, s)
      if (existing == null) attemptedId else recursiveAddEntryToExceptions(s)
    }
    val id = recursiveAddEntryToExceptions(s)
    string2Id.put(s, id)
    id
  }

  /**
   * Can only be called when there are no concurrent reads/writes.
   */
  def clear(): Unit = synchronized {
    string2Id.clear
    id2String.clear
  }

  def contains(s: String): Boolean = {
    val stringBytes = s.getBytes(utf8)
    val idCandidate = fastHash(stringBytes)
    val existing = id2String.get(idCandidate)
    if (existing == null) {
      false
    } else if (Arrays.equals(stringBytes, existing)) {
      true
    } else {
      string2Id.containsKey(stringBytes)
    }
  }

  def apply(s: String): Int = {
    val stringBytes = s.getBytes(utf8)
    val idCandidate = fastHash(stringBytes)
    val existing = id2String.get(idCandidate)
    if (existing != null && Arrays.equals(stringBytes, existing)) {
      idCandidate
    } else {
      addEntry(stringBytes, idCandidate)
    }
  }

  def apply(id: Int): String = {
    if (id == 0) {
      "*"
    } else {
      new String(id2String.get(id), utf8)
    }
  }

  def get(id: Int): Option[String] = {
    if (id == 0) {
      Some("*")
    } else {
      Option(id2String.get(id)).map(new String(_, utf8))
    }
  }

  def contains(i: Int): Boolean = {
    if (i == 0) {
      true
    } else {
      id2String.containsKey(i)
    }
  }

  def get(s: String): Option[Int] = {
    val stringBytes = s.getBytes(utf8)
    val idCandidate = fastHash(stringBytes)
    val existing = id2String.get(idCandidate)
    if (existing != null) {
      if (Arrays.equals(stringBytes, existing)) {
        Some(idCandidate)
      } else {
        val exception = string2Id.get(stringBytes)
        if (exception != 0) {
          Some(exception)
        } else {
          None
        }
      }
    } else {
      None
    }
  }

  def close(): Unit = {
    string2Id.close()
    id2String.close()
  }

  private[this] val suffixChars = 45
  private[this] val prefixChars = 5
  private[this] val fullStringUptTo = 50

  @inline private[this] def fastHash(b: Array[Byte]): Int = {
    if (b.length == 0) {
      return 0
    }
    var hash = 2147483647 // large prime.
    var i = 0
    val l = b.length - 1
    if (b.length <= fullStringUptTo) {
      while (i < l) {
        hash = MurmurHash3.mix(hash, b(i))
        i += 1
      }
    } else {
      // Skip `http://www.`
      i = if (b(0) == 'h') {
        11
      } else {
        0
      }
      val prefixEndIndex = math.min(l, i + prefixChars)
      while (i < prefixEndIndex) {
        hash = MurmurHash3.mix(hash, b(i))
        i += 1
      }
      i = math.max(i, b.length - suffixChars)
      while (i < l) {
        hash = MurmurHash3.mix(hash, b(i))
        i += 1
      }
    }
    hash = MurmurHash3.mixLast(hash, b(i))
    hash = MurmurHash3.finalizeHash(hash, 3)
    hash & Int.MaxValue
  }

  override def toString = s"HashDictionary(id2String=${id2String.size}, string2Id=${string2Id.size})"

}
