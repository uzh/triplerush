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

object LubmGenerator extends App {
  val numberOfUniversities = List(10)
  def assemblyPath = "./target/scala-2.10/triplerush-assembly-1.0-SNAPSHOT.jar"
  val kraken = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    localJarPath = assemblyPath, priority = TorquePriority.superfast)
  var evaluation = new Evaluation(
    evaluationName = s"Generating LUBM$numberOfUniversities dataset",
    executionHost = kraken)

  for (unis <- numberOfUniversities) {
    evaluation = evaluation.addEvaluationRun(generateLubm(unis))
  }

  evaluation.execute

  def generateLubm(universities: Int)(): List[Map[String, String]] = {
      // Convert the OWL files to ntriples format.
      def convert(toConvertFolder: String) {
        println("Converting LUBM files to ntriples format ...")
        for (file <- filesIn(toConvertFolder)) {
          if (file.getName.startsWith("University") && file.getName.endsWith(".owl")) {
            val nTriplesFileName = file.getAbsolutePath.replace(".owl", ".nt")
            val nTriplesFile = new File(nTriplesFileName)
            if (!nTriplesFile.exists) {
              Seq("/usr/local/Cellar/raptor/2.0.8/bin/rapper", file.getAbsolutePath, "-e", "-o", "ntriples") #>> new File(nTriplesFileName) !! (ProcessLogger(println(_)))
            }
          }
        }
        println("All files have been converted.")
      }

    // Generate raw LUBM files.
    s"java -cp uba.jar edu.lehigh.swat.bench.uba.Generator -univ $universities -onto http://swat.cse.lehigh.edu/onto/univ-bench.owl" !! (ProcessLogger(println(_)))

    // Create new directory and move files there.
    val targetFolder = createFolder(s"./lubm$universities")
    println("Moving OWL files ...")
    for (owlFile <- filesIn("./").filter(_.getName.endsWith("owl"))) {
      println(s"Moving file ${owlFile.getName} ...")
      move(owlFile, targetFolder)
    }
    move(new File("./log.txt"), targetFolder)
    println("All LUBM files have been copied.")
    convert(s"./lubm$universities")
    List()
  }

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