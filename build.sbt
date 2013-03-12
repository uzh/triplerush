import AssemblyKeys._ 
assemblySettings

/** Project */
name := "path-queries"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.1-RC3"

EclipseKeys.withSource := true

/** Dependencies */
libraryDependencies ++= Seq(
 "org.scala-lang" % "scala-library" % "2.10.1-RC3"  % "compile",
 "org.apache.jena" % "jena-arq" % "2.10.0",
 "junit" % "junit" % "4.8.2"  % "test",
 "org.specs2" % "specs2_2.10" % "1.13"  % "test",
 "org.specs2" % "classycle" % "1.4.1" % "test",
 "org.mockito" % "mockito-all" % "1.9.0"  % "test"
  )