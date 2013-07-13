///*
// *  @author Philip Stutz
// *  @author Mihaela Verman
// *  
// *  Copyright 2013 University of Zurich
// *      
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *  
// *         http://www.apache.org/licenses/LICENSE-2.0
// *  
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *  
// */
//
//package com.signalcollect.triplerush
//
//import scala.util.hashing.MurmurHash3._
//import scala.util.hashing._
//
//object BF extends BloomFilter(10000000)
//
///**
// * Simple bloom filter with #size buckets of 32 bits each.
// * Entries are mapped to buckets the same way as in a hash map.
// * Each entry added sets at least 1 and up to 3 bits within its bucket to 1.
// */
//class BloomFilter(size: Int) {
//  private[this] final val maxSize = nextPowerOfTwo(size)
//  private[this] final val ints = new Array[Int](maxSize)
//  private[this] final val mask = maxSize - 1
//  private[this] final def nextPowerOfTwo(x: Int): Int = {
//    var r = x - 1
//    r |= r >> 1
//    r |= r >> 2
//    r |= r >> 4
//    r |= r >> 8
//    r |= r >> 16
//    r + 1
//  }
//
//  //  def byteToString(byte: Byte): String = {
//  //    val sb = new StringBuilder
//  //    for (bit <- 7 to 0 by -1) {
//  //      sb.append(((byte >> bit) & 1).toString)
//  //    }
//  //    sb.toString
//  //  }
//  //
//  //  def printBits(byte: Byte) {
//  //    for (bit <- 7 to 0 by -1) {
//  //      print((byte >> bit) & 1)
//  //    }
//  //  }
//
//  //  def intToString(i: Int): String = {
//  //    val sb = new StringBuilder
//  //    for (bit <- 31 to 0 by -1) {
//  //      sb.append(((i >> bit) & 1).toString)
//  //    }
//  //    sb.toString
//  //  }
//  //
//  //  def printBits(i: Int) {
//  //    for (bit <- 31 to 0 by -1) {
//  //      print((i >> bit) & 1)
//  //    }
//  //  }
//
//  def add(item: Any) {
//    val hash = item.hashCode
//    val index = hash & mask
//    //    println(s"placed fingerprint for item ${item} at index ${index}")
//    synchronized {
//      val currentIntAtPos = ints(index)
//      //    println(s"int before write ${intToString(currentIntAtPos)}")
//      val newIntAtPos = currentIntAtPos | getIntWithUpToThreeBitsSet(byteswap32(hash))
//      //    println(s"int after write ${intToString(newIntAtPos)}")
//      ints(index) = newIntAtPos
//    }
//  }
//
//  // use first 15 bits of i as indexes of the up to 3 ints that are set
//  @inline def getIntWithUpToThreeBitsSet(i: Int): Int = {
//    val firstBitIndex = i & 0x1F
//    val firstBitSetInt = 1 << firstBitIndex
//    val secondBitIndex = i >> 5 & 0x1F
//    val secondBitSetInt = 1 << secondBitIndex
//    val thirdBitIndex = i >> 10 & 0x1F
//    val thirdBitSetInt = 1 << thirdBitIndex
//    val upToThreeIntsSet = firstBitSetInt |
//      secondBitSetInt |
//      thirdBitSetInt
//    upToThreeIntsSet
//  }
//
//  @inline def contains(item: Any): Boolean = {
//    val hash = item.hashCode
//    val index = hash & mask
//    //    println(s"looking up fingerprint for item ${item} at index ${index}")
//    val currentIntAtPos = ints(index)
//    //    println(s"int at that position ${intToString(currentIntAtPos)}")
//    val bitsThatShouldBeSet = getIntWithUpToThreeBitsSet(byteswap32(hash))
//    //    println(s"bits that should be set ${intToString(bitsThatShouldBeSet)}")
//    val extractedRelevantBits = currentIntAtPos & bitsThatShouldBeSet
//    //    println(s"extracted relevant bits ${intToString(extractedRelevantBits)}")
//    extractedRelevantBits == bitsThatShouldBeSet
//  }
//
//}
