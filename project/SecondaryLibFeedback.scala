import Dependencies._
import sbt.Keys._
import sbt._

object SecondaryLibFeedback {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(protobuf=true) ++ Seq(
    libraryDependencies ++= Seq(
      rojomaJson,
      rojomaSimpleArm,
      soqlEnvironment,
      socrataCuratorUtils,
      "org.apache.curator" % "curator-x-discovery" % "2.8.0",
      "com.socrata" %% "socrata-http-client" % "3.10.1",
      "com.typesafe" % "config" % "1.0.2"
    )
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}