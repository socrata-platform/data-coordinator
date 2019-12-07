import Dependencies._

name := "coordinator"

libraryDependencies ++= Seq(
  c3po,
  slf4jLog4j12,
  metricsScala,
  socrataThirdPartyUtils,
  jna,
  metricsJetty9,
  metricsGraphite,
  metricsJmx,
  opencsv,
  clojure,

  TestDeps.scalaCheck % "test",
  TestDeps.scalaMock  % "test"
)

test in assembly := {}

mainClass in assembly := Some("com.socrata.datacoordinator.Launch")

enablePlugins(BuildInfoPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoPackage := "com.socrata.datacoordinator"

buildInfoOptions += BuildInfoOption.ToJson