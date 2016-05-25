import sbt._

object Dependencies {

  object versions {
    val postgresql = "9.4.1207.jre7"
    val socrataHttpVersion = "3.9.2"
    val protobuf = "2.4.1"
    val rojomaJson = "3.5.0"
    val socrataCuratorUtils = "1.0.1"
    val socrataThirdpartyUtils = "4.0.1"
    val socrataUtils = "0.10.0"
    val soqlStdlib = "2.0.12"
    val sprayCaching = "1.2.2"
    val typesafeConfig = "1.2.1"
    val metricsJetty = "3.1.0"
    val metricsGraphite = "3.0.2"
    val metricsScala = "3.3.0"
    val slf4j = "1.1.0"
    val slf4jLog4j12 =  "1.7.7"
    val trove4j = "3.0.3"
    val scalaCheck = "1.11.5"
  }

  val postgresql = "org.postgresql" % "postgresql" % versions.postgresql
  val protobuf = "com.google.protobuf" % "protobuf-java" % versions.protobuf
  val rojomaJson = "com.rojoma" %% "rojoma-json-v3" % versions.rojomaJson
  val socrataHttpClient = "com.socrata" %% "socrata-http-client" % versions.socrataHttpVersion
  val socrataHttpCuratorBroker = "com.socrata" %% "socrata-http-curator-broker" % versions.socrataHttpVersion exclude("org.slf4j", "slf4j-simple") exclude ("org.jboss.netty", "netty" /* see ZOOKEEPER-1681 */)
  val socrataHttpJetty = "com.socrata" %% "socrata-http-jetty" % versions.socrataHttpVersion
  val socrataCuratorUtils = "com.socrata" %% "socrata-curator-utils" % versions.socrataCuratorUtils
  val socrataThirdpartyUtils = "com.socrata" %% "socrata-thirdparty-utils" % versions.socrataThirdpartyUtils
  val socrataUtils = "com.socrata" %% "socrata-utils" % versions.socrataUtils
  val soqlStdlib = "com.socrata" %% "soql-stdlib" % versions.soqlStdlib exclude ("javax.media", "jai_core")
  val sprayCaching = "io.spray" % "spray-caching" % versions.sprayCaching
  val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig

  val metricsJetty = "io.dropwizard.metrics" % "metrics-jetty9" % versions.metricsJetty
  // "io.dropwizard.metrics" % "metrics-graphite"          % "3.1.0",
  // See CORE-3635: use lower version of graphite to work around Graphite reconnect issues
  val metricsGraphite = "com.codahale.metrics" % "metrics-graphite" % versions.metricsGraphite exclude("com.codahale.metrics", "metrics-core")
  val metricsScala = "nl.grons" %% "metrics-scala" % versions.metricsScala

  val slf4j = "com.typesafe" %% "scalalogging-slf4j" % versions.slf4j
  val slf4jLog4j12 = "org.slf4j" % "slf4j-log4j12" % versions.slf4jLog4j12

  val trove4j = "net.sf.trove4j" % "trove4j" % versions.trove4j

  val scalaCheck = "org.scalacheck" %% "scalacheck" % versions.scalaCheck % "test"
}
