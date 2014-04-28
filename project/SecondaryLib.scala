import sbt._
import Keys._

object SecondaryLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(protobuf=true) ++ Seq(
    libraryDependencies ++= Seq(
      "net.sf.trove4j" % "trove4j" % "3.0.3",
      "com.rojoma" %% "rojoma-json" % "[2.4.3,3.0.0)",
      "com.rojoma" %% "simple-arm" % "[1.1.10,2.0.0)",
      "joda-time" % "joda-time" % "2.1",
      "org.joda" % "joda-convert" % "1.2"
    ),
    sourceGenerators in Compile <+= (sourceManaged in Compile) map { targetDir =>
      GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "ColumnId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "CopyId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "RowId")
    }
  )


  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
