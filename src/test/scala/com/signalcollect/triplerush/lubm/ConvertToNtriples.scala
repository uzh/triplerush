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