import sbt._
import sbt.Keys._
import sbtbuildinfo.BuildInfoKeys._
import sbtbuildinfo.{BuildInfoKey, BuildInfoOption}

object QueryCoordinatorHttp {

  import Dependencies._

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++
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
      libraryDependencies ++= Seq(
        protobuf,
        rojomaJson,
        socrataHttpClient,
        socrataHttpCuratorBroker,
        socrataHttpJetty,
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
