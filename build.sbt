import AssemblyKeys._ 
assemblySettings

/** Project */
name := "triplerush"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.1"

EclipseKeys.withSource := true

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "minlog-1.2.jar"}
}

/** Dependencies */
libraryDependencies ++= Seq(
 "org.scala-lang" % "scala-library" % "2.10.1"  % "compile",
 "org.apache.jena" % "jena-arq" % "2.10.0",
 "junit" % "junit" % "4.8.2"  % "test",
 "org.specs2" % "specs2_2.10" % "1.13"  % "test",
 "org.specs2" % "classycle" % "1.4.1" % "test",
 "org.mockito" % "mockito-all" % "1.9.0"  % "test"
  )