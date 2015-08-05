import Dependencies._
import sbt.Keys._
import sbt._

object Coordinator {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies <++= (scalaVersion) { (scalaVersion) =>
      Seq(
        c3po,
        metricsScala,
        slf4jLog4j12,
        socrataThirdPartyUtils,
        typesafeConfig,
        "com.socrata"    %% "socrata-http-curator-broker" % "3.3.3",
        "com.sun.jna"     % "jna"           % "3.0.9",
        "io.dropwizard.metrics" % "metrics-jetty9"   % "3.1.0",
        // "io.dropwizard.metrics" % "metrics-graphite"   % "3.1.0",
        // See CORE-3635: use lower version of graphite to work around Graphite reconnect issues
        "com.codahale.metrics" % "metrics-graphite" % "3.0.2" exclude(
                                 "com.codahale.metrics", "metrics-core"),
        "net.ceedubs"    %% "ficus"         % "1.0.0",
        "net.sf.opencsv"  % "opencsv"       % "2.3",
        "org.clojure"     % "clojure"       % "1.5.1",

        TestDeps.scalaCheck % "test",
        TestDeps.scalaMock  % "test",
        TestDeps.h2         % "test"
      )
    },
    mainClass in sbtassembly.AssemblyKeys.assembly := Some("com.socrata.datacoordinator.Launch")
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
