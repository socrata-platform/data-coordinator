import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys

object BuildSettings {
  def buildSettings: Seq[Setting[_]] =
    Defaults.itSettings ++
    Seq(
      // TODO: enable style checks
      com.socrata.sbtplugins.StylePlugin.StyleKeys.styleCheck := {},
      scalaVersion := "2.10.4",
      resolvers ++= Seq(
        "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools",
        "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release"),
      scalacOptions ++= Seq("-deprecation", "-feature")
    )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings ++
      (if (!assembly) Seq(AssemblyKeys.assembly := file(".")) else Nil)
}
