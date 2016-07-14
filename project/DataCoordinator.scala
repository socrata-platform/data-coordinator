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
        coordinator,
        secondaryLib,
        dummySecondary,
        coordinatorExternal,
        secondaryLibFeedback)
    } yield proj : ProjectReference

  private def p(name: String, settings: { def settings: Seq[Setting[_]]; def configs: Seq[Configuration] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).
      settings(settings.settings : _*).
      configs(settings.configs : _*).
      dependsOn(dependencies: _*)

  lazy val coordinatorExternal = p("coordinator-external", CoordinatorExternal)

  lazy val coordinatorLib = p("coordinatorlib", CoordinatorLib)

  lazy val coordinator = p("coordinator", Coordinator,
    coordinatorLib, coordinatorExternal).
    enablePlugins(sbtbuildinfo.BuildInfoPlugin)

  lazy val secondaryLib = p("secondarylib", SecondaryLib,
    coordinatorLib)
    // .enablePlugins(sbtbuildinfo.BuildInfoPlugin)

  lazy val dummySecondary = p("dummy-secondary", DummySecondary,
    secondaryLib % "provided")

  lazy val secondaryLibFeedback = p("secondarylib-feedback", SecondaryLibFeedback,
    secondaryLib)
}
