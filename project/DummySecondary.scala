import Dependencies._
import sbt.Keys._
import sbt._

object DummySecondary {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies += typesafeConfig % "provided"
  )

  lazy val configs = BuildSettings.projectConfigs
}
