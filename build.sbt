import AssemblyKeys._ 
assemblySettings

/** Project */
name := "triplerush"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.2"

scalacOptions ++= Seq("-optimize")

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

test in assembly := {}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "minlog-1.2.jar"}
}

/** Dependencies */
libraryDependencies ++= Seq(
 "org.scala-lang" % "scala-library" % "2.10.2"  % "compile",
 "org.apache.jena" % "jena-arq" % "2.10.0",
 "com.google.collections" % "google-collections" % "1.0" ,
 "junit" % "junit" % "4.8.2"  % "test",
 "org.specs2" %% "specs2" % "2.0-RC2"  % "test",
 "org.specs2" % "classycle" % "1.4.1" % "test",
 "org.mockito" % "mockito-all" % "1.9.0"  % "test"
  )
