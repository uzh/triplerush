import sbt._
import Keys._

object GraphsBuild extends Build {
  lazy val scCore = ProjectRef(file("../signal-collect-torque"), id = "signal-collect-torque")
  val scTriplerush = Project(id = "triplerush",
    base = file(".")) dependsOn (scCore)
}
