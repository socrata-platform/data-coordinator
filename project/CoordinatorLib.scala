import sbt._
import Keys._

import Dependencies._

object CoordinatorLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(protobuf=true) ++ Seq(
    libraryDependencies ++= Seq(
      c3po % "optional",
      jodaConvert,
      jodaTime,
      metricsScala,
      rojomaJson,
      rojomaSimpleArm,
      socrataThirdPartyUtils,
      soqlEnvironment,
      "com.google.protobuf" % "protobuf-java"            % "2.4.1",
      "com.socrata"        %% "soql-brita"               % "1.3.0",
      "com.typesafe"       %% "scalalogging-slf4j"       % "1.1.0",
      "commons-codec"       % "commons-codec"            % "1.8",
      "org.iq80.snappy"     % "snappy"                   % "0.3",
      "org.liquibase"       % "liquibase-core"           % "2.0.0",
      "org.liquibase"       % "liquibase-plugin"         % "1.9.5.0",
      "org.postgresql"      % "postgresql"               % "9.3-1102-jdbc41", // we do use postgres-specific features some places
      "org.xerial.snappy"   % "snappy-java"              % "1.1.0-M3",

      TestDeps.scalaCheck  % "test,it",
      TestDeps.scalaTest   % "it",
      TestDeps.slf4jSimple % "test,it",
      TestDeps.h2          % "test,it"
    )
  )


  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
