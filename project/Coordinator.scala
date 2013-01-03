import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object Coordinator {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
      Seq(
        "com.socrata" %% "soql-types" % "0.0.9"
      )
    }
  )
}
