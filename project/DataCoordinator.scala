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
      method <- getClass.getDeclaredMethods.toSeq
      if method.getParameterTypes.isEmpty && classOf[Project].isAssignableFrom(method.getReturnType) && method.getName != "dataCoordinator"
    } yield method.invoke(this).asInstanceOf[Project] : ProjectReference

  private def p(name: String, settings: { def settings: Seq[Setting[_]]; def configs: Seq[Configuration] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).
      settings(settings.settings : _*).
      configs(settings.configs : _*).
      dependsOn(dependencies: _*)

  lazy val coordinatorLib = p("coordinatorlib", CoordinatorLib)

  lazy val coordinatorLibSoql = p("coordinatorlib-soql", CoordinatorLibSoql,
    coordinatorLib)

  lazy val coordinator = p("coordinator", Coordinator,
    coordinatorLib, coordinatorLibSoql)

  lazy val dummySecondary = p("dummy-secondary", DummySecondary,
    coordinatorLib % "provided")
}
