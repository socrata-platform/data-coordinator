import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object Coordinator {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
      Seq(
        "com.socrata" %% "soql-types" % "0.0.9",
        "com.socrata" %% "socrata-csv" % "1.0.0",
        "org.slf4j" % "slf4j-simple" % slf4jVersion
      )
    }
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
