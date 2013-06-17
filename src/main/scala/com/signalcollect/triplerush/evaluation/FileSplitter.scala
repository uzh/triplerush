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
import com.signalcollect.nodeprovisioning.torque.TorqueHost
import com.signalcollect.nodeprovisioning.torque.TorqueJobSubmitter
import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
import com.signalcollect.nodeprovisioning.torque.TorquePriority

object FileSplitter extends App {
  val parallelism = 4
  val folder = "./lubm10"
  val splits = 2880
  def assemblyPath = "./target/scala-2.10/triplerush-assembly-1.0-SNAPSHOT.jar"
  val kraken = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    localJarPath = assemblyPath, priority = TorquePriority.superfast)
  var evaluation = new Evaluation(
    evaluationName = s"Splitting $folder",
    executionHost = kraken)

  for (modulo <- 0 until parallelism) {
    evaluation = evaluation.addEvaluationRun(splitNtriples(modulo, parallelism, folder))
  }

  evaluation.execute

  def splitNtriples(mod: Int, par: Int, sourceFolder: String)(): List[Map[String, String]] = {
    import FileOperations._

    val destinationFolder = sourceFolder + "-splits"

    val fileOutputStreams = {
      (0 until splits).toArray map { splitId =>
        if (mod == splitId % par) {
          val binaryOs = new FileOutputStream(s"$destinationFolder/$splitId.split")
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
    val initialTime = System.currentTimeMillis
    var totalTriplesSplit = 0
    val files = filesIn(sourceFolder).filter(_.getName.endsWith("binary"))
    val fileCount = files.length
    var filesProcessed = 0

    for (file <- files) {
      val startTime = System.currentTimeMillis
      filesProcessed += 1
      print(s"Processing file ${filesProcessed}/$fileCount ...")
      val triplesSplit = splitFile(file.getAbsolutePath)
      totalTriplesSplit += triplesSplit

      println(s" Done.")
      val endTime = System.currentTimeMillis
      val fileProcessingTime = (endTime - startTime).toDouble / 1000
      println(s"Processing took $fileProcessingTime seconds.")
      val totalTimeSoFar = ((endTime - initialTime).toDouble / 1000) / 3600
      println(s"Total elapsed time: $totalTimeSoFar hours.")
      val estimatedTimePerFile = totalTimeSoFar / (filesProcessed).toDouble
      val remainingFiles = fileCount - filesProcessed
      val estimatedRemaining = remainingFiles * estimatedTimePerFile
      val estimatedRemainingRounded = estimatedRemaining.floor
      println(s"Estimated remaining time for remaining files: ${estimatedRemainingRounded} hours and ${((estimatedRemaining - estimatedRemainingRounded) * 60).floor} minutes.")
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
            if (mod == patternSplit % par) {
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
      List()
  }
}