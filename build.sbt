import AssemblyKeys._

assemblySettings

net.virtualvoid.sbt.graph.Plugin.graphSettings

organization := "com.socrata"

name := "query-coordinator"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "com.socrata" %% "soql-stdlib" % "0.0.16-SNAPSHOT",
  "com.socrata" %% "socrata-thirdparty-utils" % "[2.0.0,3.0.0)",
  "com.typesafe" % "config" % "1.0.0",
  "com.socrata" %% "socrata-http-curator-broker" % "1.3.1-SNAPSHOT" exclude ("org.jboss.netty", "netty" /* see ZOOKEEPER-1681 */),
  "com.socrata" %% "socrata-internal-http" % "0.0.1-SNAPSHOT",
  "com.google.protobuf" % "protobuf-java" % "2.4.1",
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"
)

scalacOptions ++= Seq("-deprecation", "-feature")
