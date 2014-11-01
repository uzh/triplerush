import AssemblyKeys._
 
assemblySettings

/** Project */
name := "triplerush"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.11.4"

/** 
 * See https://github.com/sbt/sbt-assembly/issues/123
 */
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList(ps @ _*) if ps.last == ".DS_Store" => MergeStrategy.discard
    case other => old(other)
  }
}

scalacOptions ++= Seq("-optimize", "-Ydelambdafy:inline", "-Yclosure-elim", "-Yinline-warnings", "-Ywarn-adapted-args", "-Ywarn-inaccessible", "-feature", "-deprecation")

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

test in assembly := {}

parallelExecution in Test := false

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "minlog-1.2.jar"}
}

/** Dependencies */
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2" % "compile",
  "org.apache.jena" % "apache-jena-libs" % "2.11.1" % "test",
  "junit" % "junit" % "4.8.2"  % "test",
  "org.specs2" %% "specs2" % "2.3.11"  % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.0" % "test",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "org.easymock" % "easymock" % "3.2" % "test"
  )
