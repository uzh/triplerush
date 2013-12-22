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
import collection.JavaConversions._
import java.nio.file.Paths
import java.nio.file.Files

abstract class FileTransformation extends KrakenExecutable {
  import FileOperations._

  def parallel = false
  def nameOfTransformation: String
  def sourceFolder: String
  def destinationFolder = sourceFolder + "-" + nameOfTransformation
  def extensionTransformer(fileName: String): String
  
  def shouldTransformFile(f: File) = true

  def remoteRun(srcFolder: String, dstFolder: String)() {

    def fileInDestinationFolder(fileName: String) = dstFolder + "/" + extensionTransformer(fileName)
    lazy val target = new File(dstFolder)

    if (!target.exists) {
      target.mkdir
    }

    val files = filesIn(srcFolder).filter(shouldTransformFile)
    println("Starting file transformations...")
    if (parallel) {
      for (file <- files.par) {
        callTransform(file)
      }
    } else {
      for (file <- files) {
        callTransform(file)
      }
    }
    println("All files have been transformed.")

      def callTransform(file: File) {
        println(s"Transforming file ${file.getName}")
        transform(file, new File(fileInDestinationFolder(file.getName)))
      }

  }

  run(remoteRun(sourceFolder, destinationFolder) _)
  def transform(sourceFile: File, targetFile: File)

}

object FileOperations {
  def filesIn(folderPath: String): Array[File] = {
    val folder = new File(folderPath)
    folder.listFiles
  }

  def move(f: File, folder: File) {
    if (f.exists && folder.exists && folder.isDirectory) {
      val source = Paths.get(f.getAbsolutePath)
      val target = Paths.get(new File(folder.getAbsolutePath + "/" + f.getName).getAbsolutePath)
      Files.move(source, target)
    }
  }

  def createFolder(path: String): File = {
    val f = new File(path)
    if (!f.exists) {
      f.mkdir
    }
    f
  }
}