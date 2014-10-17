import sbt._
import Keys._

object CoordinatorLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(protobuf=true) ++ Seq(
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "com.h2database"      % "h2"            % "1.3.166" % "test,it",
      "com.mchange"         % "c3p0"          % "0.9.5-pre8" % "optional",
      "com.rojoma"         %% "rojoma-json"   % "2.4.3",
      "com.rojoma"         %% "simple-arm"    % "1.1.10",
      "com.socrata"        %% "socrata-thirdparty-utils" % "2.5.0",
      "com.socrata"        %% "soql-brita"    % "1.3.0",
      "com.socrata"        %% "soql-environment" % "0.2.1",
      "commons-codec"       % "commons-codec" % "1.8",
      "joda-time"           % "joda-time"     % "2.1",
      "nl.grons"           %% "metrics-scala" % "3.3.0",
      "org.iq80.snappy"     % "snappy"        % "0.3",
      "org.joda"            % "joda-convert"  % "1.2",
      "org.liquibase"       % "liquibase-core" % "2.0.0",
      "org.liquibase"       % "liquibase-plugin" % "1.9.5.0",
      "org.postgresql"      % "postgresql"    % "9.3-1102-jdbc41", // we do use postgres-specific features some places
      "org.scalacheck"     %% "scalacheck"    % "1.10.0" % "test,it",
      "org.scalatest"      %% "scalatest"     % "1.9.1" % "it",
      "org.slf4j"           % "slf4j-simple"  % BuildSettings.slf4jVersion % "test,it",
      "org.xerial.snappy"   % "snappy-java"   % "1.1.0-M3"
    )
  )


  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
