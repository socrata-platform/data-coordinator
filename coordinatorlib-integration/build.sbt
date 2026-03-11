import Dependencies._

name := "coordinatorlib-integration"

libraryDependencies ++= Seq(
      TestDeps.h2 % "test",
      TestDeps.scalaCheck % "test",
      TestDeps.scalaTest % "test",
      TestDeps.slf4jSimple % "test"
)

publish / skip := true

disablePlugins(AssemblyPlugin)
