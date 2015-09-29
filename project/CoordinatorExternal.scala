import Dependencies._
import sbt.Keys._
import sbt._

object CoordinatorExternal {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(protobuf=true) ++ Seq(
    libraryDependencies ++= Seq(
      jodaConvert,
      jodaTime,
      rojomaJson,
      rojomaSimpleArm,
      socrataCuratorUtils,
      socrataThirdPartyUtils,
      "com.typesafe"       %% "scalalogging-slf4j"       % "1.1.0",
      "commons-codec"       % "commons-codec"            % "1.8",
      TestDeps.scalaCheck  % "test,it",
      TestDeps.slf4jSimple % "test,it",
      TestDeps.h2          % "test,it"
    )
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
