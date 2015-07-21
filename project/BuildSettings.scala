import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin.ScoverageKeys._

import scalabuff.ScalaBuffPlugin

object BuildSettings {
  val buildSettings: Seq[Setting[_]] =
      Defaults.itSettings ++
      inConfig(UnitTest)(Defaults.testSettings) ++
      inConfig(ExploratoryTest)(Defaults.testSettings) ++
      Seq(
        // protobuff generated file(s) must be excluded from coverage
        coverageExcludedPackages := "%s;%s".format("com.socrata.datacoordinator.truth.loader.sql.messages", coverageExcludedPackages.value),
        // TODO: enable code coverage build failures
        coverageFailOnMinimum := false,
        // TODO: enable scalastyle build failures
        com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false,
        testOptions in Test ++= Seq(
          Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
        ),
        testOptions in ExploratoryTest <<= testOptions in Test,
        testOptions in UnitTest <<= (testOptions in Test) map { _ ++ Seq(Tests.Argument("-l", "Slow")) },
        scalacOptions += "-language:implicitConversions",
        libraryDependencies ++=
          Seq(
            slf4jApi
          )
      )

  lazy val buildConfigs = Configurations.default

  def projectSettings(assembly: Boolean = false, protobuf: Boolean = false): Seq[Setting[_]] =
    BuildSettings.buildSettings ++
      (if (protobuf) ScalaBuffPlugin.scalabuffSettings else Seq.empty) ++
      Seq(
        fork in test := true
      )

  lazy val projectConfigs = Seq(ScalaBuffPlugin.ScalaBuff, UnitTest, IntegrationTest, ExploratoryTest)
  lazy val ExploratoryTest = config("explore") extend (Test)
  lazy val UnitTest = config("unit") extend (Test)
}
