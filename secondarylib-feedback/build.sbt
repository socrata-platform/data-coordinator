import Dependencies._

name := "secondarylib-feedback"

libraryDependencies ++= Seq(
  rojomaJson,
  rojomaSimpleArm,
  soqlEnvironment,
  soqlStdlib,
  socrataHttpClient,
  typesafeConfig,

  TestDeps.scalaMock % "test"
)
disablePlugins(AssemblyPlugin)
