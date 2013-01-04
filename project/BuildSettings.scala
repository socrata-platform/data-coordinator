import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._
import com.socrata.socratasbt.CheckClasspath

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ socrataBuildSettings ++ Seq(
    scalaVersion := "2.10.0",
    compile in Compile <<= (compile in Compile) dependsOn (CheckClasspath.Keys.failIfConflicts in Compile),
    compile in Test <<= (compile in Test) dependsOn (CheckClasspath.Keys.failIfConflicts in Test),
    testOptions in Test ++= Seq(
      Tests.Argument("-oFD")
    ),
    testOptions in DataCoordinator.ExploratoryTest := Seq(Tests.Argument("-oFD")),
    testOptions in DataCoordinator.UnitTest ++= Seq(
      Tests.Argument("-oFD"),
      Tests.Argument(TestFrameworks.ScalaTest, "-l", "Slow") // option "-l" will exclude the specified tags
    )
  )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    BuildSettings.buildSettings ++ socrataProjectSettings(assembly = assembly) ++ Seq(
      slf4jVersion := "1.7.2",
      fork in test := true,
      test in Test <<= (test in Test) dependsOn (test in DataCoordinator.IntegrationTestClone)
    )
}
