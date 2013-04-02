import sbt._
import Keys._

object BuildSettings {
  val buildSettings: Seq[Setting[_]] =
    Defaults.defaultSettings ++
      Defaults.itSettings ++
      inConfig(UnitTest)(Defaults.testSettings) ++
      inConfig(ExploratoryTest)(Defaults.testSettings) ++
      Seq(
        organization := "com.socrata",
        version := "0.0.1-SNAPSHOT",
        scalaVersion := "2.10.0",
        testOptions in Test ++= Seq(
          Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
        ),
        testOptions in ExploratoryTest <<= testOptions in Test,
        testOptions in UnitTest <<= (testOptions in Test) map { _ ++ Seq(Tests.Argument("-l", "Slow")) },
        scalacOptions <++= (scalaVersion) map {
          case s if s.startsWith("2.9.") => Seq("-encoding", "UTF-8", "-g:vars", "-unchecked", "-deprecation")
          case s if s.startsWith("2.10.") => Seq("-encoding", "UTF-8", "-g:vars", "-deprecation", "-feature", "-language:implicitConversions")
        },
        javacOptions ++= Seq("-encoding", "UTF-8", "-g", "-Xlint:unchecked", "-Xlint:deprecation", "-Xmaxwarns", "999999"),
        ivyXML := // com.rojoma and com.socrata have binary compat guarantees
          <dependencies>
            <conflict org="com.socrata" manager="latest-compatible"/>
            <conflict org="com.rojoma" manager="latest-compatible"/>
          </dependencies>,
        libraryDependencies <++= (scalaVersion) { sv =>
          Seq(
            "org.slf4j" % "slf4j-api" % slf4jVersion,
            "org.scalatest" %% "scalatest" % scalaTestVersion(sv) % "test"
          )
        }
      )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    BuildSettings.buildSettings ++
      Seq(
        fork in test := true,
        test in Test <<= (test in Test) dependsOn (test in IntegrationTest)
      )

  lazy val projectConfigs = Configurations.default ++ Seq(UnitTest, IntegrationTest, ExploratoryTest)
  lazy val ExploratoryTest = config("explore") extend (Test)
  lazy val UnitTest = config("unit") extend (Test)

  val slf4jVersion = "1.7.5"

  def scalaTestVersion(sv: String) = sv match {
    case s if s.startsWith("2.8.") => "1.8"
    case _ => "1.9.1"
  }
}
