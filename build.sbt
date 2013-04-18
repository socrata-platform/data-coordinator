organization := "com.socrata"

name := "query-coordinator"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "com.socrata" %% "config-utils" % "[1.0.0,2.0.0)",
  "com.socrata" %% "socrata-http-curator-broker" % "[1.3.0,2.0.0)"
)
