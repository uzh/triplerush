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
import com.signalcollect.triplerush.TriplePattern
import java.io.FileOutputStream
import java.io.DataOutputStream
import collection.JavaConversions._
import java.io.DataInputStream
import java.io.EOFException
import scala.collection.mutable.HashSet

object DuplicateFilter extends FileTransformation with Serializable {

  override def executionHost = kraken
  def nameOfTransformation = "filtered"
  def sourceFolder = s"${args(0)}-binary-splits"
  override def destinationFolder = sourceFolder.replace("binary", nameOfTransformation)
  override def shouldTransformFile(f: File) = f.getName.endsWith(".split")
  override def extensionTransformer(fileName: String) = fileName.replace(".split", ".filtered-split")
  override def transform(sourceFile: File, targetFile: File) {
    val is = new FileInputStream(sourceFile)
    val dis = new DataInputStream(is)
    val tripleSet = HashSet[TriplePattern]()
    try {
      while (true) {
        val sId = dis.readInt
        val pId = dis.readInt
        val oId = dis.readInt
        val pattern = TriplePattern(sId, pId, oId)
        assert(pattern.isFullyBound)
        tripleSet.add(pattern)
      }
    } catch {
      case done: EOFException =>
        dis.close
        is.close
      case t: Throwable =>
        throw t
    }
    val splitFileOutputStream = new FileOutputStream(targetFile)
    val splitDataOutputStream = new DataOutputStream(splitFileOutputStream)
    for (triplePattern <- tripleSet) {
      splitDataOutputStream.writeInt(triplePattern.s)
      splitDataOutputStream.writeInt(triplePattern.p)
      splitDataOutputStream.writeInt(triplePattern.o)
    }
    splitDataOutputStream.close
    splitFileOutputStream.close
    println(tripleSet.size)
  }

}