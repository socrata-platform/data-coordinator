import sbt._
import sbt.Keys._

object SecondarySelector {

  import Dependencies._

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++
    Seq(
      name := "secondary-selector",
      libraryDependencies ++= Seq(
        socrataThirdpartyUtils,
        typesafeConfig,
        metricsScala,
        slf4j)
    )
}
