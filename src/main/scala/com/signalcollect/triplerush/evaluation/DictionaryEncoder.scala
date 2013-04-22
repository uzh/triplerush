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
  val toEncodeFolderName = "lubm10240"
  val existingDictionaryPath: Option[String] = Some("./lubm160/dictionary.txt")
  val newDictionaryPath = "./lubm10240/dictionary.txt"
  val dictionary = new HashMap[String, Int]()
  var highestUsedId = 0
  if (existingDictionaryPath.isDefined){
      val dictionaryFile = Source.fromFile(existingDictionaryPath.get)
  for (line <- dictionaryFile.getLines) {
    val entry = line.split(" -> ")
    if (entry.length == 2) {
      dictionary.put(entry(1), entry(0).toInt)
    } else if (entry.length != 0) {
      throw new Exception(s"Failed to parse line $line, was parsed to ${entry.toList}.")
    }
  
  }}
    
  var nextId = highestUsedId + 1

  val originalExtension = ".nt"
  val textExtension = ".txt"
  val binaryExtension = ".binary"
  val folder = new File(toEncodeFolderName)
  for (file <- folder.listFiles) {
    if (file.getName.startsWith("University") && file.getName.endsWith(originalExtension)) {
      encodeFile(file.getAbsolutePath)
    }
  }
  val dictionaryOs = new FileOutputStream(newDictionaryPath)
  val dictionaryDos = new DataOutputStream(dictionaryOs)
  for (entry <- dictionary) {
    dictionaryDos.writeChars(s"${entry._1} -> ${entry._2}\n")
  }
  dictionaryDos.close
  dictionaryOs.close

  def register(entry: String): Int = {
    if (dictionary.containsKey(entry)) {
      dictionary.get(entry)
    } else {
      val idForEntry = nextId
      dictionary.put(entry, idForEntry)
      nextId += 1
      idForEntry
    }
  }

  def encodeFile(path: String) {
    val is = new FileInputStream(path)
    val textOs = new FileOutputStream(path.replace(originalExtension, textExtension))
    val textDos = new DataOutputStream(textOs)
    val binaryOs = new FileOutputStream(path.replace(originalExtension, binaryExtension))
    val binaryDos = new DataOutputStream(binaryOs)
    val nxp = new NxParser(is)
    println(s"Reading triples from $path ...")
    var triplesEncoded = 0
    while (nxp.hasNext) {
      val triple = nxp.next
      val subjectString = triple(0).toString
      if (!subjectString.startsWith("file:///Users")) {
        val predicateString = triple(1).toString
        val objectString = triple(2).toString
        val sId = register(subjectString)
        val pId = register(predicateString)
        val oId = register(objectString)
        textDos.writeChars(s"$sId $pId $oId\n")
        binaryDos.write(sId)
        binaryDos.write(pId)
        binaryDos.write(oId)
        triplesEncoded += 1
        if (triplesEncoded % 10000 == 0) {
          println(s"Loaded $triplesEncoded triples from file $path ...")
        }
      }
    }
    println(s"Done loading triples from $path. Loaded a total of $triplesEncoded triples.")
    binaryDos.close
    binaryOs.close
    textDos.close
    textOs.close
    is.close
  }

}