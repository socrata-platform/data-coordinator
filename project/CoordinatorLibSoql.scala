import sbt._
import Keys._

object CoordinatorLibSoql {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools",
    libraryDependencies ++= Seq(
      "com.socrata" %% "soql-types" % "[0.2.1,1.0.0)",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
    )
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}

