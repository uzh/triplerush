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

import java.io.DataOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.HashMap
import collection.JavaConversions._
import scala.io.Source
import org.semanticweb.yars.nx.parser.NxParser
import java.io.OutputStreamWriter
import com.signalcollect.triplerush.evaluation.QueryEncoding

object DictionaryEncoderWithBase extends KrakenExecutable with Serializable {
//  override def executionHost = local
  run(BaseEncoder.encode(args(0)) _)
}

case object BaseEncoder extends Serializable {
  def encode(sourceFolderBaseName: String)() {
    import FileOperations._

    val sourceFolderName = s"./${sourceFolderBaseName}-nt"
    val targetFolderName = sourceFolderName.replace("nt", "binary")
    createFolder(targetFolderName)
    val source = new File(sourceFolderName)
    val target = new File(targetFolderName)
    val dictionary: HashMap[String, Int] = new HashMap[String, Int]()
    for (entry <- QueryEncoding.m) {
      dictionary.put(entry._1, entry._2)
    }
    var nextId = dictionary.values.max + 1
    val dictionaryPath = s"$targetFolderName/dictionary.txt"

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

    //    println("Reading base dictionary ...")
    //    val dictionaryFile = Source.fromFile(baseDictionaryPath)
    //    for (line <- dictionaryFile.getLines) {
    //      val entry = line.split(" ")
    //      if (entry.length == 3) {
    //        val id = entry(2).toInt
    //        nextId = math.max(nextId, id + 1)
    //        dictionary.put(entry(0), id)
    //        println("Added entry " + entry(0))
    //      } else if (entry.length != 0) {
    //        throw new Exception(s"Failed to parse line $line, was parsed to ${entry.toList}.")
    //      }
    //    }

    println("Encoding files ...")

    val sourceFiles = filesIn(sourceFolderName).
      filter(_.getName.endsWith(".nt")).
      sorted

    for (src <- sourceFiles) {
      val trg = new File(targetFolderName + "/" + src.getName.replace(".nt", ".binary"))
      println(s"Encoding file $src.")
      encodeFile(src, trg)
    }

    println(s"${sourceFiles.length} files have been encoded, dictionary contains ${nextId} entries.")

    println("Writing dictionary.")
    val dictionaryOs = new FileOutputStream(dictionaryPath)
    val writer = new OutputStreamWriter(dictionaryOs, "UTF8")
    for (entry <- dictionary) {
      writer.write(s"${entry._1} -> ${entry._2}\n")
    }
    writer.close
    dictionaryOs.close

  }
}
