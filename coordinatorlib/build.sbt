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
      metricsJetty9,
      metricsGraphite,
      metricsJmx,
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

Compile / sourceGenerators += Def.task {
  val targetDir = (Compile/sourceManaged).value
  GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "ColumnId") ++
    GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "CopyId") ++
    GenLongLikeMap(targetDir, "com.socrata.datacoordinator.util.collection", "com.socrata.datacoordinator.id", "RowId")
}

Compile / managedSourceDirectories += sourceManaged.value / "scala"

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile/sourceManaged).value / "protobuf"
)

disablePlugins(AssemblyPlugin)
