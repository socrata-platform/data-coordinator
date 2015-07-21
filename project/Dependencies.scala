import sbt._

object Dependencies {
  object versions {
    val c3po            = "0.9.5-pre9"
    val h2              = "1.3.166"
    val metricsScala    = "3.3.0"
    val jodaConvert     = "1.2"
    val jodaTime        = "2.1"
    val rojomaJson      = "3.2.2"
    val rojomaSimpleArm = "1.1.10"
    val scalaCheck      = "1.11.0"
    val scalaMock       = "3.2"
    val slf4j           = "1.7.5"
    val soqlReference   = "0.5.3"
    val thirdPartyUtils = "3.1.2"
    val typesafeConfig  = "1.2.1"
  }

  val c3po = "com.mchange" % "c3p0" % versions.c3po

  val metricsScala = "nl.grons" %% "metrics-scala" % versions.metricsScala

  val rojomaJson      = "com.rojoma" %% "rojoma-json-v3" % versions.rojomaJson
  val rojomaSimpleArm = "com.rojoma" %% "simple-arm"     % versions.rojomaSimpleArm

  val slf4jApi     = "org.slf4j" % "slf4j-api"     % versions.slf4j
  val slf4jLog4j12 = "org.slf4j" % "slf4j-log4j12" % versions.slf4j

  val jodaConvert = "org.joda"  % "joda-convert" % versions.jodaConvert
  val jodaTime    = "joda-time" % "joda-time"    % versions.jodaTime

  val socrataThirdPartyUtils = "com.socrata" %% "socrata-thirdparty-utils" % versions.thirdPartyUtils

  val soqlEnvironment = "com.socrata" %% "soql-environment" % versions.soqlReference
  val soqlTypes       = "com.socrata" %% "soql-types"       % versions.soqlReference

  val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig

  object TestDeps {
    val h2          = "com.h2database"  % "h2"           % versions.h2
    val scalaCheck  = "org.scalacheck" %% "scalacheck"   % versions.scalaCheck
    val scalaMock   = "org.scalamock"  %% "scalamock-scalatest-support" % versions.scalaMock
    val slf4jSimple = "org.slf4j"       % "slf4j-simple" % versions.slf4j
  }
}
