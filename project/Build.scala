import sbt._
import sbtassembly.AssemblyKeys
import sbtbuildinfo.BuildInfoPlugin

object Build extends sbt.Build {
  lazy val build = Project(
    "query-coordinator-build",
    file("."),
    settings = Seq(AssemblyKeys.assembly := file(".")) // no root assembly
  ).settings(BuildSettings.buildSettings : _*)
   .aggregate(queryCoordinator, secondarySelector)

  private def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).settings(settings.settings : _*).configs(IntegrationTest).dependsOn(dependencies: _*)

  lazy val queryCoordinator = p("query-coordinator", QueryCoordinator, secondarySelector)
    .enablePlugins(BuildInfoPlugin)

  lazy val secondarySelector = p("secondary-selector", SecondarySelector)
}
