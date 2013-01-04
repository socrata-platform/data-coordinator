import sbt._

object DataCoordinator extends Build {
  lazy val dataCoordinator = Project(
    "data-coordinator",
    file("."),
    settings = BuildSettings.buildSettings
  ).aggregate (allOtherProjects: _*)

  private def allOtherProjects =
    for {
      method <- getClass.getDeclaredMethods.toSeq
      if method.getParameterTypes.isEmpty && classOf[Project].isAssignableFrom(method.getReturnType) && method.getName != "dataCoordinator"
    } yield method.invoke(this).asInstanceOf[Project] : ProjectReference

  private def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name), settings = settings.settings).
      dependsOn(dependencies: _*).
<<<<<<< HEAD
      configs(UnitTest, IntegrationTestClone, ExploratoryTest).
      settings( Defaults.itSettings : _*).
      settings( inConfig(ExploratoryTest)(Defaults.testSettings) : _*)
=======
      configs(UnitTest).
      settings( inConfig(UnitTest)(Defaults.testSettings) : _*)
>>>>>>> 86b7b1d... test tags!

  lazy val coordinatorLib = p("coordinatorlib", CoordinatorLib)

  lazy val perfTest = p("perftest", PerfTest,
    coordinatorLib)

  lazy val coordinator = p("coordinator", Coordinator,
    coordinatorLib)

<<<<<<< HEAD
  lazy val ExploratoryTest = config("explore") extend (Test)
  lazy val UnitTest = config("unit") extend (Test)
  lazy val IntegrationTestClone = config("integration") extend (IntegrationTest)
=======
  lazy val UnitTest = config("unit") extend (Test)
>>>>>>> 86b7b1d... test tags!
}
