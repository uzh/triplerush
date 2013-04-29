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

object FileSplitter extends App {
  val modulos = (args map (_.toInt)).toSet
  val toSplitFolderName = s"lubm160"
  val destinationFolderName = "lubm160-splits"
  val splits = 2880
  val fileOutputStreams = {
    (0 until splits).toArray map { splitId =>
      if (modulos.contains(splitId % 4)) {
        val binaryOs = new FileOutputStream(s"./$destinationFolderName/$splitId.split")
        binaryOs
      } else {
        null.asInstanceOf[FileOutputStream]
      }
    }
  }
  val dataOutputStreams = fileOutputStreams map { fileOutputStream =>
    if (fileOutputStream != null) {
      val binaryDos = new DataOutputStream(fileOutputStream)
      binaryDos
    } else {
      null.asInstanceOf[DataOutputStream]
    }
  }
  val binaryExtension = ".binary"
  val folder = new File(toSplitFolderName)
  val initialTime = System.currentTimeMillis
  var totalTriplesSplit = 0
  for (university <- (0 until 160)) {
    val startTime = System.currentTimeMillis
    print(s"Processing university ${university + 1}/160 ...")
    for (subfile <- (0 to 99)) {
      val potentialFileName = s"./lubm160/University${university}_$subfile.binary"
      val potentialFile = new File(potentialFileName)
      if (potentialFile.exists) {
        val triplesSplit = splitFile(potentialFile.getAbsolutePath)
        totalTriplesSplit += triplesSplit
      }
    }
    println(s" Done.")
    val endTime = System.currentTimeMillis
    val uniProcessingTime = (endTime - startTime).toDouble / 1000
    println(s"Processing took $uniProcessingTime seconds.")
    val totalTimeSoFar = ((endTime - initialTime).toDouble / 1000) / 3600
    println(s"Total elapsed time: $totalTimeSoFar hours.")
    val estimatedTimePerUniversity = totalTimeSoFar / (university + 1).toDouble
    val remainingUniversities = (160 - (university + 1))
    val estimatedRemaining = remainingUniversities * estimatedTimePerUniversity
    val estimatedRemainingRounded = estimatedRemaining.floor
    println(s"Estimated remaining time for remaining universities: ${estimatedRemainingRounded} hours and ${((estimatedRemaining - estimatedRemainingRounded) * 60).floor} minutes.")
    println(s"Triples processed so far: $totalTriplesSplit")
  }
  dataOutputStreams.foreach { s => if (s != null) s.close }
  fileOutputStreams.foreach { s => if (s != null) s.close }

  def splitFile(path: String): Int = {
    val is = new FileInputStream(path)
    val dis = new DataInputStream(is)
    var triplesSplit = 0
    try {
      while (true) {
        val sId = dis.readInt
        val pId = dis.readInt
        val oId = dis.readInt
        val pattern = TriplePattern(sId, pId, oId)
        val patternSplit = {
          val potentiallyNegativeSplitId = pattern.hashCode % splits
          if (potentiallyNegativeSplitId >= 0) {
            potentiallyNegativeSplitId
          } else {
            if (potentiallyNegativeSplitId == Int.MinValue) {
              // Special case,-Int.MinValue == Int.MinValue
              0
            } else {
              -potentiallyNegativeSplitId
            }
          }
        }
        if (modulos.contains(patternSplit % 4)) {
          val splitStream = dataOutputStreams(patternSplit)
          splitStream.writeInt(sId)
          splitStream.writeInt(pId)
          splitStream.writeInt(oId)
          triplesSplit += 1
        }
      }
    } catch {
      case done: EOFException =>
        dis.close
        is.close
      case t: Throwable =>
        throw t
    }
    triplesSplit
  }

}