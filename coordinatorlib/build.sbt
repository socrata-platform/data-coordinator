import Dependencies._

name := "coordinatorlib"

libraryDependencies ++= Seq(
      activemq,
      c3po,
      commonsCodec,
      eurybates,
      jodaConvert,
      jodaTime,
      liquibaseCore,
      liquibasePlugin,
      metricsScala,
      postgresql,
      rojomaJson,
      rojomaSimpleArm,
      snappyIq80,
      snappyXerial,
      socrataCuratorUtils,
      socrataHttpClient,
      socrataHttpCuratorBroker,
      socrataThirdPartyUtils,
      soqlBrita,
      soqlEnvironment,
      soqlStdlib,
      soqlTypes,
      trove4j,
      typesafeConfig,

      TestDeps.h2 % "test,it",
      TestDeps.scalaCheck % "test,it",
      TestDeps.scalaTest % "test,it",
      TestDeps.slf4jSimple % "test,it"
)

sourceGenerators in Compile += Def.task {
  val targetDir = (sourceManaged in Compile).value
  GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "ColumnId") ++
    GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "CopyId") ++
    GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "RowId")
}

managedSourceDirectories in Compile += sourceManaged.value / "scala"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value / "protobuf"
)

disablePlugins(AssemblyPlugin)
