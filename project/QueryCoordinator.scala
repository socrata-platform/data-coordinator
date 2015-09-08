import sbt._
import sbt.Keys._
import sbtbuildinfo.BuildInfoKeys._
import sbtbuildinfo.{BuildInfoKey, BuildInfoOption}
import scoverage.ScoverageSbtPlugin

object QueryCoordinator {

  import Dependencies._

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(true) ++
    Seq(
      name := "query-coordinator",
      buildInfoKeys := Seq[BuildInfoKey](
        name,
        version,
        scalaVersion,
        sbtVersion,
        BuildInfoKey.action("buildTime") { System.currentTimeMillis },
        BuildInfoKey.action("revision") { gitSha }),
      buildInfoPackage := "com.socrata.querycoordinator",
      buildInfoOptions += BuildInfoOption.ToMap,
      ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 30,
      libraryDependencies ++= Seq(
        protobuf,
        rojomaJson,
        socrataHttpClient,
        socrataHttpCuratorBroker,
        socrataHttpJetty,
        socrataCuratorUtils,
        socrataThirdpartyUtils,
        soqlStdlib,
        typesafeConfig,
        metricsJetty,
        metricsGraphite,
        metricsScala,
        slf4j,
        slf4jLog4j12,
        trove4j,
        scalaCheck)
    )

  lazy val gitSha = Process(Seq("git", "describe", "--always", "--dirty", "--long", "--abbrev=10")).!!.stripLineEnd
}
