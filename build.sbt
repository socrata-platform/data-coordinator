ThisBuild / organization := "com.socrata"

ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oFD")

ThisBuild / scalaVersion := "2.12.21"

ThisBuild / resolvers += "socrata maven" at "https://repo.socrata.com/artifactory/libs-release"

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature")

ThisBuild / evictionErrorLevel := Level.Warn

val coordinatorExternal = (project in file("coordinator-external"))

val coordinatorlib = (project in file("coordinatorlib"))

val coordinatorlibIntegration = (project in file("coordinatorlib-integration")).
  dependsOn(coordinatorlib)

val coordinator = (project in file("coordinator")).
  dependsOn(coordinatorlib, coordinatorExternal)

val secondarylib = (project in file("secondarylib")).
  dependsOn(coordinatorlib)

val dummySecondary = (project in file("dummy-secondary")).
  dependsOn(secondarylib % "provided")

val secondarylibFeedback = (project in file("secondarylib-feedback")).
  dependsOn(secondarylib)

val datasetMover = (project in file("dataset-mover")).
  dependsOn(coordinator)

publish / skip := true

releaseProcess -= ReleaseTransformations.publishArtifacts

disablePlugins(AssemblyPlugin)
