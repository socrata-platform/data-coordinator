import Dependencies._

name := "coordinator-external"

libraryDependencies ++= Seq(
  jodaConvert,
  jodaTime,
  rojomaJson,
  rojomaSimpleArm,
  socrataCuratorUtils,
  socrataThirdPartyUtils,
  commonsCodec,
  TestDeps.scalaCheck  % "test",
  TestDeps.slf4jSimple % "test",
  TestDeps.h2          % "test"
)
disablePlugins(AssemblyPlugin)
