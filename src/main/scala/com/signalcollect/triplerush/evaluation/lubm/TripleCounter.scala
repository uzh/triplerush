/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
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

package com.signalcollect.triplerush.evaluation.lubm
import java.io.File
import java.io.FileInputStream
import java.io.EOFException
import java.io.DataInputStream

object TripleCounter extends App {
  val toCountFolderName = args(0)
  val splits = 2880
  val folder = new File(toCountFolderName)
  val initialTime = System.currentTimeMillis
  var triplesCounted = 0
  for (split <- (0 until splits)) {
    val startTime = System.currentTimeMillis
    print(s"Processing split $split/$splits ...")
    val triplesInSplit = countSplit(split)
    triplesCounted += triplesInSplit
    println(s" Done.")
    val endTime = System.currentTimeMillis
    val uniProcessingTime = (endTime - startTime).toDouble / 1000
    println(s"Processing took $uniProcessingTime seconds.")
    val totalTimeSoFar = ((endTime - initialTime).toDouble / 1000) / 3600
    val totalTimeSoFarRounded = totalTimeSoFar.floor
    println(s"Total elapsed time: $totalTimeSoFarRounded hours and ${((totalTimeSoFar - totalTimeSoFarRounded) * 60).floor.toInt} minutes.")
    val estimatedTimePerSplit = totalTimeSoFar / (split + 1).toDouble
    val remainingSplits = (splits - (split + 1))
    val estimatedRemaining = remainingSplits * estimatedTimePerSplit
    val estimatedRemainingRounded = estimatedRemaining.floor
    println(s"Estimated remaining time for remaining splits: ${estimatedRemainingRounded.toInt} hours and ${((estimatedRemaining - estimatedRemainingRounded) * 60).floor.toInt} minutes.")
    println(s"# triples after filtering so far: $triplesCounted")
  }

  def countSplit(split: Int): Int = {
    val is = new FileInputStream(s"./$toCountFolderName/$split.filtered-split")
    val dis = new DataInputStream(is)
    var triplesCounted = 0
    try {
      while (true) {
        val sId = dis.readInt
        val pId = dis.readInt
        val oId = dis.readInt
        triplesCounted += 1
      }
    } catch {
      case done: EOFException =>
        dis.close
        is.close
      case t: Throwable =>
        throw t
    }
    triplesCounted
  }

}