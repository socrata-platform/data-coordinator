import sbt._
import Keys._

object CoordinatorLibSoql {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "soql-types" % "0.0.16-SNAPSHOT",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
    )
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}

