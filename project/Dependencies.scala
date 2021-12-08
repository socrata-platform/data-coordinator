import sbt._

object Dependencies {
  object versions {
    val activemq        = "5.13.3"
    val c3po            = "0.9.5-pre9"
    val clojure         = "1.5.1"
    val commonsCodec    = "1.8"
    val eurybates       = "2.1.0"
    val h2              = "1.3.166"
    val jna             = "3.0.9"
    val jodaConvert     = "1.2"
    val jodaTime        = "2.1"
    val liquibaseCore   = "2.0.0"
    val liquibasePlugin = "1.9.5.0"
    val metrics         = "4.1.2"
    val metricsScala    = "4.1.1"
    val opencsv         = "2.3"
    val postgresql      = "42.2.5"
    val rojomaJson      = "3.10.0"
    val rojomaSimpleArm = "2.1.0"
    val scalaCheck      = "1.14.0"
    val scalaMock       = "3.6.0"
    val scalaTest       = "3.0.8"
    val slf4j           = "1.7.5"
    val snappyIq80      = "0.3"
    val snappyXerial    = "1.1.0-M3"
    val socrataCuratorUtils = "1.2.0"
    val socrataHttp     = "3.13.3"
    val socrataThirdPartyUtils = "5.0.0"
    val soqlBrita       = "1.4.1"
    val soqlReference   = "3.14.2"
    val trove4j         = "3.0.3"
    val typesafeConfig  = "1.2.1"
  }

  val activemq = "org.apache.activemq" % "activemq-client" % versions.activemq exclude("org.apache", "commons.logging") exclude("commons-logging", "commons-logging") exclude("org.springframework", "spring-context")

  val c3po = "com.mchange" % "c3p0" % versions.c3po

  val clojure = "org.clojure" % "clojure" % versions.clojure

  val commonsCodec = "commons-codec" % "commons-codec" % versions.commonsCodec

  val eurybates = "com.socrata" %% "eurybates" % versions.eurybates

  val jna = "com.sun.jna" % "jna" % versions.jna

  val liquibaseCore = "org.liquibase" % "liquibase-core" % versions.liquibaseCore
  val liquibasePlugin = "org.liquibase" % "liquibase-plugin" % versions.liquibasePlugin

  val metricsScala = "nl.grons" %% "metrics4-scala" % versions.metricsScala
  val metricsJetty9 = "io.dropwizard.metrics" % "metrics-jetty9" % versions.metrics
  val metricsGraphite = "io.dropwizard.metrics" % "metrics-graphite" % versions.metrics
  val metricsJmx = "io.dropwizard.metrics" % "metrics-jmx" % versions.metrics

  val opencsv = "net.sf.opencsv" % "opencsv" % "2.3"

  val postgresql = "org.postgresql" % "postgresql" % versions.postgresql

  val rojomaJson      = "com.rojoma" %% "rojoma-json-v3" % versions.rojomaJson
  val rojomaSimpleArm = "com.rojoma" %% "simple-arm-v2"  % versions.rojomaSimpleArm

  val slf4jApi     = "org.slf4j" % "slf4j-api"     % versions.slf4j
  val slf4jLog4j12 = "org.slf4j" % "slf4j-log4j12" % versions.slf4j

  val jodaConvert = "org.joda"  % "joda-convert" % versions.jodaConvert
  val jodaTime    = "joda-time" % "joda-time"    % versions.jodaTime

  val snappyIq80 = "org.iq80.snappy" % "snappy" % versions.snappyIq80
  val snappyXerial = "org.xerial.snappy" % "snappy-java" % versions.snappyXerial

  val socrataCuratorUtils = "com.socrata" %% "socrata-curator-utils" % versions.socrataCuratorUtils
  val socrataHttpCuratorBroker = "com.socrata"    %% "socrata-http-curator-broker" % versions.socrataHttp // brings in org.apache.curator
  val socrataHttpClient = "com.socrata"    %% "socrata-http-client" % versions.socrataHttp // brings in org.apache.curator
  val socrataThirdPartyUtils = "com.socrata" %% "socrata-thirdparty-utils" % versions.socrataThirdPartyUtils

  val soqlBrita = "com.socrata" %% "soql-brita" % versions.soqlBrita

  val soqlEnvironment = "com.socrata" %% "soql-environment" % versions.soqlReference
  val soqlTypes       = "com.socrata" %% "soql-types"       % versions.soqlReference
  val soqlStdlib      = "com.socrata" %% "soql-stdlib"      % versions.soqlReference

  val trove4j = "net.sf.trove4j" % "trove4j" % versions.trove4j

  val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig

  object TestDeps {
    val h2          = "com.h2database"  % "h2"           % versions.h2
    val scalaCheck  = "org.scalacheck" %% "scalacheck"   % versions.scalaCheck
    val scalaMock   = "org.scalamock"  %% "scalamock-scalatest-support" % versions.scalaMock
    val scalaTest   = "org.scalatest"  %% "scalatest"    % versions.scalaTest
    val slf4jSimple = "org.slf4j"       % "slf4j-simple" % versions.slf4j
  }
}
