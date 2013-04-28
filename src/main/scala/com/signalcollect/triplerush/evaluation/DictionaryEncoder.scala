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

object DictionaryEncoder extends App {
  val toEncodeFolderName = "lubm10240" //"lubm160"
  val existingDictionaryPath: Option[String] = Some("./lubm160/dictionary.txt")
  val newDictionaryPath = "./lubm10240/dictionary.txt" //"./lubm160/dictionary.txt"
  val baseDictionary = new HashMap[String, Int]()
  val universityDictionary = new HashMap[String, Int]()
  val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
  var highestUsedId = 0
  if (existingDictionaryPath.isDefined) {
    val dictionaryFile = Source.fromFile(existingDictionaryPath.get, "UTF-16")
    for (line <- dictionaryFile.getLines) {
      val entry = line.split(" ")
      if (entry.length == 3) {
        val id = entry(2).toInt
        highestUsedId = math.max(highestUsedId, id)
        baseDictionary.put(entry(0), id)
      } else if (entry.length != 0) {
        throw new Exception(s"Failed to parse line $line, was parsed to ${entry.toList}.")
      }
    }
  }
  var nextId = highestUsedId + 1
  val originalExtension = ".nt"
  val textExtension = ".txt"
  val binaryExtension = ".binary"
  val folder = new File(toEncodeFolderName)
  val initialTime = System.currentTimeMillis
  for (university <- (0 until 10240)) {
    val startTime = System.currentTimeMillis
    print(s"Processing university ${university+1}/10240 ...")
    for (subfile <- (0 to 99)) {
      val potentialFileName = s"./lubm10240/University${university}_$subfile.nt"
      val potentialFile = new File(potentialFileName)
      if (potentialFile.exists) {
        encodeFile(potentialFile.getAbsolutePath)
      }
    }
    universityDictionary.clear
    println(s" Done.")
    val endTime = System.currentTimeMillis
    val uniProcessingTime = (endTime - startTime).toDouble / 1000
    println(s"Processing took $uniProcessingTime seconds.")
    val totalTimeSoFar = ((endTime - initialTime).toDouble / 1000) / 3600
    println(s"Total elapsed time: $totalTimeSoFar hours.")
    val estimatedTimePerUniversity = totalTimeSoFar / (university + 1).toDouble
    val remainingUniversities = (10240 - (university + 1))
    val estimatedRemaining = remainingUniversities * estimatedTimePerUniversity
    println(s"Estimated remaining time for remaining unversities: $estimatedRemaining hours.")
  }
  //  val dictionaryOs = new FileOutputStream(newDictionaryPath)
  //  val dictionaryDos = new DataOutputStream(dictionaryOs)
  //  for (entry <- baseDictionary) {
  //    dictionaryDos.writeChars(s"${entry._1} -> ${entry._2}\n")
  //  }
  //  dictionaryDos.close
  //  dictionaryOs.close

  def register(entry: String): Int = {
    val id = baseDictionary.get(entry)
    if (id != null && id != 0) {
      id
    } else {
      val localId = universityDictionary.get(entry)
      if (localId != null && localId != 0) {
        localId
      } else {
        val idForEntry = nextId
        universityDictionary.put(entry, idForEntry)
        nextId += 1
        idForEntry
      }
    }
  }

  def encodeFile(path: String) {
    val is = new FileInputStream(path)
    val binaryOs = new FileOutputStream(path.replace(originalExtension, binaryExtension))
    val binaryDos = new DataOutputStream(binaryOs)
    val nxp = new NxParser(is)
    while (nxp.hasNext) {
      val triple = nxp.next
      val subjectString = triple(0).toString
      if (!subjectString.startsWith("file:///Users")) {
        val predicateString = triple(1).toString
        val objectString = triple(2).toString
        val sId = register(subjectString)
        val pId = register(predicateString)
        val oId = register(objectString)
        binaryDos.writeInt(sId)
        binaryDos.writeInt(pId)
        binaryDos.writeInt(oId)
      }
    }
    binaryDos.close
    binaryOs.close
    is.close
  }

}