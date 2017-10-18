import Dependencies._
import sbt.Keys._
import sbt._

object CoordinatorLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(protobuf=true) ++ Seq(
    libraryDependencies ++= Seq(
      c3po,
      jodaConvert,
      jodaTime,
      metricsScala,
      rojomaJson,
      rojomaSimpleArm,
      socrataThirdPartyUtils,
      soqlEnvironment,
      soqlStdlib,
      soqlTypes,
      typesafeConfig,
      "net.ceedubs" %% "ficus" % "1.0.0",
      "com.google.protobuf" % "protobuf-java" % "2.6.1",
      "com.socrata" %% "soql-brita" % "1.3.0",
      "com.socrata" %% "socrata-http-client" % "3.10.1",
      "com.typesafe" %% "scalalogging-slf4j" % "1.1.0",
      "commons-codec" % "commons-codec" % "1.8",
      "net.sf.trove4j" % "trove4j" % "3.0.3",
      "org.iq80.snappy" % "snappy" % "0.3",
      "org.liquibase" % "liquibase-core" % "2.0.0",
      "org.liquibase" % "liquibase-plugin" % "1.9.5.0",
      "org.postgresql" % "postgresql" % "9.4.1212", // we do use postgres-specific features some places
      "org.xerial.snappy" % "snappy-java" % "1.1.0-M3",
      "com.socrata" %% "eurybates" % "2.0.0",
      "org.apache.activemq" % "activemq-core" % "5.7.0" exclude("org.apache", "commons.logging") exclude("commons-logging", "commons-logging") exclude("org.springframework", "spring-context"),

      TestDeps.scalaTest % "test,it",
      TestDeps.scalaCheck % "test,it",
      TestDeps.slf4jSimple % "test,it",
      TestDeps.h2 % "test,it"
    ),

    sourceGenerators in Compile <+= (sourceManaged in Compile) map { targetDir =>
      GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "ColumnId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "CopyId") ++
        GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "RowId")
    }
  )

  lazy val configs: Seq[Configuration] = BuildSettings.projectConfigs
}
