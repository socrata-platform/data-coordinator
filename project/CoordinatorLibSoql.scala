import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object CoordinatorLibSoql {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= (scalaVersion, slf4jVersion) { (scalaVersion, slf4jVersion) =>
      Seq(
        "com.socrata" %% "soql-types" % "[0.0.9,0.0.15)"
      )
    }
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}

