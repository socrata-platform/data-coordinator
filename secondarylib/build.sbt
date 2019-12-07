import Dependencies._

name := "secondarylib"

libraryDependencies ++= Seq(
  slf4jLog4j12,

  TestDeps.scalaMock % "test",
  TestDeps.h2 % "test"
)
disablePlugins(AssemblyPlugin)
