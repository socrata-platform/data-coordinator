import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._

object Coordinator {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    resourceGenerators in Compile <+= (baseDirectory, resourceManaged in Compile, streams) map buildNativeLib,
    libraryDependencies <++= (scalaVersion) { (scalaVersion) =>
      Seq(
        "com.sun.jna" % "jna" % "3.0.9",
        "com.socrata" %% "socrata-thirdparty-utils" % "[2.0.0,3.0.0)",
        "net.sf.opencsv" % "opencsv" % "2.3",
        "com.typesafe" % "config" % "1.0.1",
        "com.socrata" %% "socrata-http-curator-broker" % "1.3.1-SNAPSHOT",
        "org.slf4j" % "slf4j-log4j12" % BuildSettings.slf4jVersion,
        "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
      )
    },
    mainClass in assembly := Some("com.socrata.datacoordinator.Launch")
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
}
