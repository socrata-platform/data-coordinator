import sbt._
import Keys._

import Dependencies._

object DummySecondary {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies += typesafeConfig % "provided"
  )

  lazy val configs = BuildSettings.projectConfigs
}
