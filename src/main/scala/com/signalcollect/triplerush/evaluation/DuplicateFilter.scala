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

package com.signalcollect.triplerush.evaluation

import java.io.File
import org.semanticweb.yars.nx.parser.NxParser
import com.signalcollect.triplerush.Mapping
import java.io.FileInputStream
import com.signalcollect.triplerush.TriplePattern
import java.io.FileOutputStream
import java.io.DataOutputStream
import java.util.HashMap
import collection.JavaConversions._
import scala.io.Source
import java.io.DataInputStream
import java.io.EOFException
import scala.collection.mutable.HashSet

object DuplicateFilter extends App {

  val modulos = (args map (_.toInt)).toSet
  val toSplitFolderName = "lubm10240-splits"
  val destinationFolderName = "lubm10240-filtered-splits"
  val splits = 2880
  val folder = new File(toSplitFolderName)
  val initialTime = System.currentTimeMillis
  var totalTriplesAfterFilter = 0
  for (split <- (0 until splits)) {
    val startTime = System.currentTimeMillis
    print(s"Processing split $split/$splits ...")
    val triplesAfterFilter = filterSplit(split)
    totalTriplesAfterFilter += triplesAfterFilter
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
    println(s"# triples after filtering so far: $totalTriplesAfterFilter")
  }

  def filterSplit(split: Int): Int = {
    val is = new FileInputStream(s"./$toSplitFolderName/$split.split")
    val dis = new DataInputStream(is)
    val tripleSet = HashSet[TriplePattern]()
    try {
      while (true) {
        val sId = dis.readInt
        val pId = dis.readInt
        val oId = dis.readInt
        val pattern = TriplePattern(sId, pId, oId)
        tripleSet.add(pattern)
      }
    } catch {
      case done: EOFException =>
        dis.close
        is.close
      case t: Throwable =>
        throw t
    }
    val splitFileOutputStream = new FileOutputStream(s"./$destinationFolderName/$split.filtered-split")
    val splitDataOutputStream = new DataOutputStream(splitFileOutputStream)
    for (triplePattern <- tripleSet) {
      splitDataOutputStream.writeInt(triplePattern.s)
      splitDataOutputStream.writeInt(triplePattern.p)
      splitDataOutputStream.writeInt(triplePattern.o)
    }
    splitDataOutputStream.close
    splitFileOutputStream.close
    tripleSet.size
  }

}