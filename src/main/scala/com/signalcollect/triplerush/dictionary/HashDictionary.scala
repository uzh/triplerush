package com.signalcollect.triplerush.dictionary

import org.mapdb.DBMaker
import org.mapdb.DBMaker.Maker
import org.mapdb.BTreeKeySerializer
import org.mapdb.Serializer
import java.nio.charset.Charset
import java.util.Arrays
import scala.util.hashing.MurmurHash3

final class HashDictionary(
    val id2StringNodeSize: Int = 200,
    val string2IdNodeSize: Int = 200,
    dbMaker: Maker = DBMaker
      .memoryUnsafeDB
      .closeOnJvmShutdown
      .transactionDisable
      .asyncWriteEnable
      .asyncWriteQueueSize(4096)
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

  var allIdsTakenUpTo = 0

  def initialize(): Unit = {
    id2String.put(0, "*".getBytes(utf8))
  }

  def getNextId(): Int = {
    var keepLooking = true
    while (keepLooking) {
      allIdsTakenUpTo += 1
      keepLooking = id2String.containsKey(allIdsTakenUpTo)
    }
    allIdsTakenUpTo
  }

  // idCandidate is the hashCode of the byte array
  private[this] def addEntry(s: Array[Byte], idCandidate: Int): Int = {
    val existing = id2String.putIfAbsent(idCandidate, s)
    if (existing == null) {
      idCandidate
    } else {
      if (Arrays.equals(s, existing)) {
        idCandidate // existing
      } else {
        addEntryToExceptions(s) // collision
      }
    }
  }

  def addEntryToExceptions(s: Array[Byte]): Int = synchronized {
    val id = getNextId
    id2String.put(id, s)
    string2Id.put(s, id)
    id
  }

  /**
   * Can only be called when there are no concurrent reads/writes.
   */
  def clear(): Unit = {
    string2Id.clear()
    id2String.clear()
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

  private[this] val hashChars = 10

  @inline private[this] def fastHash(b: Array[Byte]): Int = {
    if (b.length == 0) {
      0
    } else {
      var hash = 2147483647 // large prime.
      val l = b.length - 1
      var i = math.max(0, b.length - hashChars)
      while (i < l) {
        hash = MurmurHash3.mix(hash, b(i))
        i += 1
      }
      hash = MurmurHash3.mixLast(hash, b(i))
      hash = MurmurHash3.finalizeHash(hash, 3)
      hash & Int.MaxValue
    }
  }

  override def toString = s"HashDictionary(id2String=${id2String.size}, string2Id=${string2Id.size})"

}
