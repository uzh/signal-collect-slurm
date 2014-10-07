import sbt._
import Keys._

object GraphsBuild extends Build {
  lazy val scCore = ProjectRef(file("../signal-collect"), id = "signal-collect")
  val scSlurm = Project(id = "signal-collect-slurm",
    base = file(".")) dependsOn (scCore)
}
