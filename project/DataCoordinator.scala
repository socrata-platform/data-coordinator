import sbt._

object DataCoordinator extends Build {
  lazy val dataCoordinator = Project(
    "data-coordinator",
    file(".")
  ).settings(BuildSettings.buildSettings : _*)
   .configs(BuildSettings.buildConfigs : _*)
   .aggregate (allOtherProjects: _*)

  private def allOtherProjects =
    for {
      proj <- Seq(coordinatorLib,
        coordinatorLibSoql,
        coordinator,
        secondaryLib,
        dummySecondary)
    } yield proj : ProjectReference

  private def p(name: String, settings: { def settings: Seq[Setting[_]]; def configs: Seq[Configuration] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).
      settings(settings.settings : _*).
      configs(settings.configs : _*).
      dependsOn(dependencies: _*)

  lazy val coordinatorLib = p("coordinatorlib", CoordinatorLib,
    secondaryLib)

  lazy val coordinatorLibSoql = p("coordinatorlib-soql", CoordinatorLibSoql,
    coordinatorLib)

  lazy val coordinator = p("coordinator", Coordinator,
    coordinatorLib, coordinatorLibSoql)

  lazy val secondaryLib = p("secondarylib", SecondaryLib)

  lazy val dummySecondary = p("dummy-secondary", DummySecondary,
    secondaryLib % "provided")
}
