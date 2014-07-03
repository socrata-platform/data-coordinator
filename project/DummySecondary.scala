import sbt._
import Keys._

object DummySecondary {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies += "com.typesafe" % "config" % "1.2.1" % "provided"
  )

  lazy val configs = BuildSettings.projectConfigs
}
