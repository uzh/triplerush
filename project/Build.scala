import sbt._
import Keys._

object GraphsBuild extends Build {
  lazy val scCore = ProjectRef(file("../signal-collect-torque"), id = "signal-collect-torque")
  lazy val scSlurm = ProjectRef(file("../signal-collect-slurm"), id = "signal-collect-slurm")
  val scTriplerush = Project(id = "triplerush",
    base = file(".")) dependsOn (scCore, scSlurm)
}
