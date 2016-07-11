import Dependencies._
import sbt.Keys._
import sbt._

object SecondaryLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      slf4jLog4j12,
      TestDeps.scalaMock  % "test",
      TestDeps.h2         % "test"
    )
  )
  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
