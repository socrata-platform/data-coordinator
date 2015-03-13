import sbt._
import Keys._

import com.rojoma.simplearm.util._
import com.rojoma.json.v3.util.JsonUtil.writeJson

import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy

import Dependencies._

object Coordinator {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, version in Compile, scalaVersion in Compile) map genVersion,
    libraryDependencies <++= (scalaVersion) { (scalaVersion) =>
      Seq(
        c3po,
        metricsScala,
        slf4jLog4j12,
        socrataThirdPartyUtils,
        typesafeConfig,

        "com.socrata"    %% "socrata-http-curator-broker" % "3.0.1",
        "com.sun.jna"     % "jna"           % "3.0.9",
        "io.dropwizard.metrics" % "metrics-jetty9"   % "3.1.0",
        // "io.dropwizard.metrics" % "metrics-graphite"   % "3.1.0",
        // See CORE-3635: use lower version of graphite to work around Graphite reconnect issues
        "com.codahale.metrics" % "metrics-graphite" % "3.0.2" exclude(
                                 "com.codahale.metrics", "metrics-core"),
        "net.ceedubs"    %% "ficus"         % "1.0.0",
        "net.sf.opencsv"  % "opencsv"       % "2.3",
        "org.clojure"     % "clojure"       % "1.5.1",

        TestDeps.scalaCheck % "test"
      )
    },
    mainClass in assembly := Some("com.socrata.datacoordinator.Launch"),
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { old =>
      {
        case "about.html" => MergeStrategy.rename
        case x => old(x)
      }
    }
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs

  def genVersion(resourceManaged: File, name: String, version: String, scalaVersion: String): Seq[File] = {
    val file = resourceManaged / "data-coordinator-version.json"

    val revision = Process(Seq("git", "describe", "--always", "--dirty", "--long")).!!.split("\n")(0)

    val result = Map(
      "service" -> name,
      "version" -> version,
      "revision" -> revision,
      "scala" -> scalaVersion
    ) ++ sys.env.get("BUILD_TAG").map("build" -> _)

    resourceManaged.mkdirs()
    for {
      stream <- managed(new java.io.FileOutputStream(file))
      w <- managed(new java.io.OutputStreamWriter(stream, "UTF-8"))
    } {
      writeJson(w, result, pretty = true)
      w.write("\n")
    }

    Seq(file)
  }
}
