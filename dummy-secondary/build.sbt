import Dependencies._

name := "dummy-secondary"

libraryDependencies += typesafeConfig % "provided"

disablePlugins(AssemblyPlugin)

publish / skip := true
