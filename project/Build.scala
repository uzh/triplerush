import sbt._
import Keys._

object TripleRushBuild extends Build {
  lazy val triplerush = Project(id = "triplerush", base = file("."))
}
