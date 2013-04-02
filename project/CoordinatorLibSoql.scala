import sbt._
import Keys._

object CoordinatorLibSoql {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "soql-types" % "0.0.14"
    )
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}

