import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt
import scalabuff.ScalaBuffPlugin

object BuildSettings {
  val buildSettings: Seq[Setting[_]] =
    SocrataCloudbeesSbt.socrataBuildSettings ++
      Defaults.itSettings ++
      inConfig(UnitTest)(Defaults.testSettings) ++
      inConfig(ExploratoryTest)(Defaults.testSettings) ++
      Seq(
        scalaVersion := "2.10.4",
        testOptions in Test ++= Seq(
          Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
        ),
        testOptions in ExploratoryTest <<= testOptions in Test,
        testOptions in UnitTest <<= (testOptions in Test) map { _ ++ Seq(Tests.Argument("-l", "Slow")) },
        scalacOptions += "-language:implicitConversions",
        libraryDependencies <++= (scalaVersion) { sv =>
          Seq(
            "org.slf4j" % "slf4j-api" % slf4jVersion,
            "org.scalatest" %% "scalatest" % scalaTestVersion(sv) % "test"
          )
        }
      )

  lazy val buildConfigs = Configurations.default

  def projectSettings(assembly: Boolean = false, protobuf: Boolean = false): Seq[Setting[_]] =
    BuildSettings.buildSettings ++
      SocrataCloudbeesSbt.socrataProjectSettings(assembly) ++
      (if(protobuf) ScalaBuffPlugin.scalabuffSettings else Seq.empty) ++
      Seq(
        fork in test := true
      )

  lazy val projectConfigs = Seq(ScalaBuffPlugin.ScalaBuff, UnitTest, IntegrationTest, ExploratoryTest)
  lazy val ExploratoryTest = config("explore") extend (Test)
  lazy val UnitTest = config("unit") extend (Test)

  val slf4jVersion = "1.7.5"

  def scalaTestVersion(sv: String) = sv match {
    case s if s.startsWith("2.8.") => "1.8"
    case _ => "1.9.1"
  }
}
