import AssemblyKeys._ 
assemblySettings

/** Project */
name := "triplerush"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.11.2"

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
  "org.scala-lang" % "scala-library" % "2.11.2"  % "compile",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2" % "compile",
  "org.apache.jena" % "apache-jena-libs" % "2.11.1" % "test",
  "junit" % "junit" % "4.8.2"  % "test",
  "org.specs2" %% "specs2" % "2.3.11"  % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.0" % "test",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "org.easymock" % "easymock" % "3.2" % "test"
  )
