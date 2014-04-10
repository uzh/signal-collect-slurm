import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-slurm"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-optimize", "-Yinline-warnings", "-feature", "-deprecation", "-Xelide-below", "INFO" )

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

test in assembly := {}

parallelExecution in Test := false

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "minlog-1.2.jar"}
}

/** Dependencies */
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.10.4"  % "compile",
  "ch.ethz.ganymed" % "ganymed-ssh2" % "build210"  % "compile",
  "commons-codec" % "commons-codec" % "1.7"  % "compile"
  )
