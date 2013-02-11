import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object Coordinator {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    resourceGenerators in Compile <+= (baseDirectory, resourceManaged in Compile, streams) map buildNativeLib,
    libraryDependencies <++= (scalaVersion, slf4jVersion) { (scalaVersion, slf4jVersion) =>
      Seq(
        "org.scala-lang" % "scala-reflect" % scalaVersion,
        "com.sun.jna" % "jna" % "3.0.9",
        "com.typesafe" % "config" % "1.0.0",
        "com.socrata" %% "soql-types" % "0.0.9",
        "com.socrata" %% "socrata-csv" % "1.0.0",
        "org.slf4j" % "slf4j-simple" % slf4jVersion,
        "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
      )
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
}
