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

import java.io.DataOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.HashMap
import collection.JavaConversions._

import scala.io.Source

import org.semanticweb.yars.nx.parser.NxParser

object DictionaryEncoderWithBase extends KrakenExecutable with Serializable {
  runOnKraken(Encoder.encode(args(0)) _)
}

object BaseEncoder {
  def encode(sourceFolderBaseName: String)() {
    import FileOperations._

    val sourceFolderName = s"./${sourceFolderBaseName}-nt"
    val targetFolderName = sourceFolderName.replace("nt", "binary")
    createFolder(targetFolderName)
    val source = new File(sourceFolderName)
    val target = new File(targetFolderName)
    var nextId = 0
    val dictionaryPath = s"$targetFolderName/dictionary.txt"
    val baseDictionaryPath = "./dictionary.txt"
    val dictionary = new HashMap[String, Int]()
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"

      def register(entry: String): Int = {
        val id = dictionary.get(entry)
        if (id != 0) {
          id
        } else {
          val idForEntry = nextId
          dictionary.put(entry, idForEntry)
          nextId += 1
          idForEntry
        }
      }

      def encodeFile(source: File, target: File) {
        val is = new FileInputStream(source)
        val binaryOs = new FileOutputStream(target)
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

    println("Reading base dictionary ...")
    val dictionaryFile = Source.fromFile(baseDictionaryPath, "UTF-16")
    for (line <- dictionaryFile.getLines) {
      val entry = line.split(" ")
      if (entry.length == 3) {
        val id = entry(2).toInt
        nextId = math.max(nextId, id + 1)
        dictionary.put(entry(0), id)
      } else if (entry.length != 0) {
        throw new Exception(s"Failed to parse line $line, was parsed to ${entry.toList}.")
      }
    }

    println("Encoding files ...")

    val sourceFiles = filesIn(sourceFolderName)

    for (src <- sourceFiles) {
      val trg = new File(targetFolderName + "/" + src.getName.replace(".nt", ".binary"))
      println(s"Encoding file $src.")
      encodeFile(src, trg)
    }

    println(s"${sourceFiles.length} files have been encoded, dictionary contains ${nextId + 1} entries.")

    println("Writing dictionary.")
    val dictionaryOs = new FileOutputStream(dictionaryPath)
    val dictionaryDos = new DataOutputStream(dictionaryOs)
    for (entry <- dictionary) {
      dictionaryDos.writeChars(s"${entry._1} -> ${entry._2}\n")
    }
    dictionaryDos.close
    dictionaryOs.close

  }
}
