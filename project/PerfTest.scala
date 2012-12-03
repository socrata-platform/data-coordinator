import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._
import sbtassembly.Plugin._
import AssemblyKeys._

object PerfTest {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
      Seq(
        "org.slf4j" % "slf4j-simple" % slf4jVersion
      )
    },
    mainClass in assembly := Some("com.socrata.datacoordinator.truth.loader.sql.perf.ExecutePlan")
  )
}
