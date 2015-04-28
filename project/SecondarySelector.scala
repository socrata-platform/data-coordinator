import sbt._
import sbt.Keys._
import scoverage.ScoverageSbtPlugin

object SecondarySelector {

  import Dependencies._

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++
    Seq(
      name := "secondary-selector",
      ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0,
      libraryDependencies ++= Seq(
        socrataThirdpartyUtils,
        typesafeConfig,
        metricsScala,
        slf4j)
    )
}
