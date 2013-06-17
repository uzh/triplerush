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
import scala.sys.process._
import com.signalcollect.nodeprovisioning.torque.TorqueHost
import com.signalcollect.nodeprovisioning.torque.TorqueJobSubmitter
import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
import com.signalcollect.nodeprovisioning.torque.TorquePriority
import java.nio.file.{ Files, Path, Paths }

object NtriplesConverter extends FileTransformation with App {

  def nameOfTransformation = "nt"
  def sourceFolder = "lubm10"
  
  override def shouldTransformFile(f: File) = f.getName.endsWith(".owl")
  override def fileInDestinationFolder(fileName: String) = destinationFolder + "/" + fileName.replace(".owl", ".nt")
  // Convert the OWL files to ntriples format.
  def transform(sourceFile: File, targetFile: File) {
    if (!targetFile.exists) {
      Seq("/usr/local/Cellar/raptor/2.0.8/bin/rapper", sourceFile.getAbsolutePath, "-e", "-o", "ntriples") #>> targetFile !! (ProcessLogger(println(_)))
    }
  }

}

