import sbt._
import sbtbuildinfo.BuildInfoPlugin

object Build extends sbt.Build {
  lazy val build = Project(
    "query-coordinator",
    file(".")
  ).settings(BuildSettings.buildSettings : _*)
   .aggregate(queryCoordinatorHttp, secondarySelector)

  private def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).settings(settings.settings : _*).configs(IntegrationTest).dependsOn(dependencies: _*)

  lazy val queryCoordinatorHttp = p("query-coordinator-http", QueryCoordinatorHttp, secondarySelector)
    .enablePlugins(BuildInfoPlugin)

  lazy val secondarySelector = p("secondary-selector", SecondarySelector)
}
