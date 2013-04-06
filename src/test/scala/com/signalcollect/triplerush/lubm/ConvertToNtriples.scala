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

package com.signalcollect.triplerush.lubm

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

object ConvertToNtriples extends App {
  val toRenameFolder = "lubm160"
  val folder = new File(toRenameFolder)
  for (file <- folder.listFiles) {
    if (file.getName.startsWith("University") && file.getName.endsWith(".owl")) {
      //Runtime.getRuntime.exec(Array(s"""/usr/local/Cellar/raptor/2.0.8/bin/rapper ${file.getAbsolutePath} -e -o ntriples >> ${file.getAbsolutePath.replace(".owl", ".nt")}"""))
      //Seq("/usr/local/Cellar/raptor/2.0.8/bin/rapper", file.getAbsolutePath, "-e", "-o", "ntriples", ">>", file.getAbsolutePath.replace(".owl", ".nt")) !!(ProcessLogger(println(_)))
      Seq("/usr/local/Cellar/raptor/2.0.8/bin/rapper", file.getAbsolutePath, "-e", "-o", "ntriples") #>> new File(file.getAbsolutePath.replace(".owl", ".nt")) !! (ProcessLogger(println(_)))
      //val command = s"/usr/local/Cellar/raptor/2.0.8/bin/rapper ${file.getAbsolutePath} -e -o ntriples >> ${file.getAbsolutePath.replace(".owl", ".nt")}"
      //println(command)
      //println(command !!(ProcessLogger(println(_))))
    }
  }
}