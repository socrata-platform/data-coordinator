import sbt._

import com.socrata.socratasbt._

object DataCoordinator extends Build {
  lazy val dataCoordinator = Project(
    "data-coordinator",
    file("."),
    settings = Defaults.defaultSettings ++ SocrataSbt.socrataBuildSettings
  ) aggregate (allOtherProjects: _*)

  private def allOtherProjects =
    for {
      method <- getClass.getDeclaredMethods.toSeq
      if method.getParameterTypes.isEmpty && classOf[Project].isAssignableFrom(method.getReturnType) && method.getName != "dataCoordinator"
    } yield method.invoke(this).asInstanceOf[Project] : ProjectReference

  private def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name), settings = settings.settings) dependsOn(dependencies: _*)

  lazy val coordinatorLib = p("coordinatorlib", CoordinatorLib)

  lazy val perfTest = p("perftest", PerfTest,
    coordinatorLib)
}
