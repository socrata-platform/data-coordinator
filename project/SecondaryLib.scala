import Dependencies._
import sbt.Keys._
import sbt._

object SecondaryLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(protobuf=true) ++ Seq(
    libraryDependencies ++= Seq(
      jodaConvert,
      jodaTime,
      rojomaJson,
      rojomaSimpleArm,
      soqlEnvironment,
      "net.sf.trove4j" % "trove4j" % "3.0.3"
    ),
    sourceGenerators in Compile <+= (sourceManaged in Compile) map { targetDir =>
      GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "ColumnId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "CopyId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "RowId")
    }
  )


  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
