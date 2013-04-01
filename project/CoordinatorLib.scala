import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object CoordinatorLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
      Seq(
        "com.socrata" %% "soql-brita" % "[1.2.0,2.0.0)",
        "com.socrata" %% "soql-environment" % "0.0.13",
        "com.rojoma" %% "rojoma-json" % "[2.3.0,3.0.0)",
        "com.rojoma" %% "simple-arm" % "1.1.10",
        "joda-time" % "joda-time" % "2.1",
        "org.joda" % "joda-convert" % "1.2",
        "net.sf.trove4j" % "trove4j" % "3.0.3",
        "org.xerial.snappy" % "snappy-java" % "1.0.5-M3",
        "postgresql" % "postgresql" % "9.1-901-1.jdbc4", // we do use postgres-specific features some places
        "com.mchange" % "c3p0" % "0.9.2.1" % "optional",
        "com.google.protobuf" % "protobuf-java" % "2.4.1",
        "com.h2database" % "h2" % "1.3.166" % "test,it",
        "org.scalacheck" %% "scalacheck" % "1.10.0" % "test,it",
        "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
        "org.scalatest" %% "scalatest" % "1.9.1" % "it"
      )
    },
    sourceGenerators in Compile <+= (sourceManaged in Compile) map { targetDir =>
      GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "ColumnId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "DatasetId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "CopyId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "RowId")
    }
  )


  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
