import sbt._
import Keys._

import com.rojoma.simplearm.util._
import com.rojoma.json.util.JsonUtil.writeJson

import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy

object Coordinator {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    resourceGenerators in Compile <+= (baseDirectory, resourceManaged in Compile, streams) map buildNativeLib,
    resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, version in Compile, scalaVersion in Compile) map genVersion,
    libraryDependencies <++= (scalaVersion) { (scalaVersion) =>
      Seq(
        "net.ceedubs"    %% "ficus"         % "1.0.0",
        "com.mchange"     % "c3p0"          % "0.9.5-pre8",
        "com.socrata"    %% "socrata-http-curator-broker" % "2.3.1",
        "com.socrata"    %% "socrata-thirdparty-utils" % "2.5.0",
        "com.sun.jna"     % "jna"           % "3.0.9",
        "com.typesafe"    % "config"        % "1.2.1",
        "io.dropwizard.metrics" % "metrics-jetty9"   % "3.1.0",
        "io.dropwizard.metrics" % "metrics-graphite"   % "3.1.0",
        "net.sf.opencsv"  % "opencsv"       % "2.3",
        "nl.grons"       %% "metrics-scala" % "3.3.0",
        "org.clojure"     % "clojure"       % "1.5.1",
        "org.scalacheck" %% "scalacheck"    % "1.10.0" % "test",
        "org.slf4j"       % "slf4j-log4j12" % BuildSettings.slf4jVersion
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

  def buildNativeLib(baseDir: File, resourceManaged: File, s: TaskStreams) =
    if(sys.props("os.name") == "Linux" && sys.props("os.arch") == "amd64") {
      val target = resourceManaged / "com" / "socrata" / "datacoordinator" / "packets" / "network" / "native-library"
      target.getParentFile.mkdirs()
      val result = Process(List("make", "SOURCEDIR=" + baseDir.absolutePath, "TARGET=" + target.absolutePath), baseDir) ! s.log
      if(result != 0) sys.error("Native library build failure")
      Seq(target)
    } else {
      Nil
    }

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
