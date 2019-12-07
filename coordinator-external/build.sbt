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
  TestDeps.scalaCheck  % "test,it",
  TestDeps.slf4jSimple % "test,it",
  TestDeps.h2          % "test,it"
)
disablePlugins(AssemblyPlugin)
