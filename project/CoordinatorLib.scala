import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object CoordinatorLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
      Seq(
        "com.socrata" %% "socrata-id" % "2.1.1",
        "com.rojoma" %% "rojoma-json" % "2.1.0",
        "com.rojoma" %% "simple-arm" % "1.1.10",
        "joda-time" % "joda-time" % "2.1",
        "org.joda" % "joda-convert" % "1.2",
        "net.sf.trove4j" % "trove4j" % "3.0.3",
        "org.xerial.snappy" % "snappy-java" % "1.0.4.1",
        "postgresql" % "postgresql" % "9.1-901-1.jdbc4", // we do use postgres-specific features some places
        "com.google.protobuf" % "protobuf-java" % "2.4.1",
        "com.h2database" % "h2" % "1.3.166" % "test",
        "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
        "org.slf4j" % "slf4j-simple" % slf4jVersion % "test"
      )
    }
  )
}
