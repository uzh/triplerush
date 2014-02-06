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
import java.io.FileInputStream
import com.signalcollect.triplerush.TriplePattern
import java.io.FileOutputStream
import java.io.DataOutputStream
import collection.JavaConversions._
import java.io.DataInputStream
import java.io.EOFException
import com.signalcollect.nodeprovisioning.torque.TorqueHost
import com.signalcollect.nodeprovisioning.torque.TorqueJobSubmitter
import com.signalcollect.nodeprovisioning.torque.TorquePriority
import FileOperations.createFolder
import FileOperations.filesIn
import com.signalcollect.triplerush.evaluation.Evaluation
import scala.Array.canBuildFrom
import com.signalcollect.triplerush.TripleMapper
import com.signalcollect.nodeprovisioning.torque.LocalHost

object FileSplitter extends App {

  def assemblyPath = "./target/scala-2.10/triplerush-assembly-1.0-SNAPSHOT.jar"
  val kraken = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    coresPerNode = 1,
    localJarPath = assemblyPath, priority = TorquePriority.fast)
  val local = new LocalHost
  var evaluation = new Evaluation(
    executionHost = kraken)

  for (modulo <- 0 until 4) { // Has to match parallelism variable inside splitting function.
    evaluation = evaluation.addEvaluationRun(splitNtriples(modulo, args(0)))
  }

  evaluation.execute

  def splitNtriples(mod: Int, baseSourceFolderName: String)(): List[Map[String, String]] = {
    import FileOperations._
    println("Modulo is: " + mod)
    val splits = 2880
    val mapper = new TripleMapper[Any](numberOfNodes = splits, workersPerNode = 1)
    val parallelism = 4
    val sourceFolder = s"./$baseSourceFolderName-binary"
    val destinationFolder = sourceFolder + "-splits"
    createFolder(destinationFolder)

    val fileOutputStreams = {
      (0 until splits).toArray map { splitId =>
        if (mod == splitId % parallelism) {
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
      filesProcessed += 1
      print(s"Processing file ${filesProcessed}/$fileCount ...")
      val triplesSplit = splitFile(file.getAbsolutePath)
      totalTriplesSplit += triplesSplit
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
          if (!pattern.isFullyBound) {
            println(s"Problem: $pattern, triple #${triplesSplit + 1} in file $path is not fully bound.")
          } else {
            val patternSplit = mapper.getWorkerIdForVertexId(pattern)
            if (mod == patternSplit % parallelism) {
              val splitStream = dataOutputStreams(patternSplit)
              splitStream.writeInt(sId)
              splitStream.writeInt(pId)
              splitStream.writeInt(oId)
              triplesSplit += 1
            }
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