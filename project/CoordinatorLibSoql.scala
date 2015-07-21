import Dependencies._
import sbt.Keys._
import sbt._

object CoordinatorLibSoql {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools",
    libraryDependencies ++= Seq(
      soqlTypes,
      TestDeps.scalaCheck % "test"
    )
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}

