import sbt._
import Keys._

object GraphsBuild extends Build {
  lazy val scCore = ProjectRef(file("../signal-collect"), id = "signal-collect")
  val scTriplerush = Project(id = "triplerush",
    base = file(".")) dependsOn (scCore)
}
