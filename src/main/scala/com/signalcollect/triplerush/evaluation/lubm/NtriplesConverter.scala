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
import scala.sys.process._

object NtriplesConverter extends FileTransformation with Serializable {

  def nameOfTransformation = "nt"
  def sourceFolder = s"./${args(0)}"
  
  override def shouldTransformFile(f: File) = f.getName.endsWith(".owl")
  override def extensionTransformer(fileName: String) = fileName.replace(".owl", ".nt")
  // Convert the OWL files to ntriples format.
  def transform(sourceFile: File, targetFile: File) {
    if (!targetFile.exists) {
      Seq("/usr/bin/rapper", sourceFile.getAbsolutePath, "-e", "-o", "ntriples") #>> targetFile !! (ProcessLogger(println(_)))
    }
  }

}
