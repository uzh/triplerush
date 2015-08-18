import AssemblyKeys._

assemblySettings

/** Project */
name := "triplerush"

version := "4.1.1-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.11.7"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

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

assembleArtifact in packageScala := true

parallelExecution in Test := false

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

jarName in assembly := "triplerush.jar"

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {_.data.getName == "minlog-1.2.jar"}
}

/** Dependencies */
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
  "com.signalcollect" %% "signal-collect" % "4.0.0" % "compile",
  "org.apache.jena" % "jena-arq" % "3.0.0" % "compile",
  "org.apache.jena" % "jena-core" % "3.0.0" % "compile",
  "org.apache.jena" % "jena-core" % "3.0.0" % "test" classifier "tests",
  "org.apache.jena" % "apache-jena-libs" % "3.0.0" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
  "org.easymock" % "easymock" % "3.3.1" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.openrdf.sesame" % "sesame-sparql-testsuite" % "4.0.0-M1" notTransitive
)

pomExtra := (
 <url>https://github.com/uzh/triplerush</url>
 <scm>
   <url>git@github.com:uzh/triplerush.git</url>
   <connection>scm:git:git@github.com:uzh/triplerush.git</connection>
 </scm>
 <developers>
   <developer>
     <id>pstutz</id>
     <name>Philip Stutz</name>
     <url>https://github.com/pstutz</url>
   </developer>
   <developer>
     <id>bibekp</id>
     <name>Bibek Paudel</name>
     <url>https://github.com/bibekp</url>
   </developer>
   <developer>
     <id>elaverman</id>
     <name>Mihaela Verman</name>
     <url>https://github.com/elaverman</url>
   </developer>
 </developers>)
