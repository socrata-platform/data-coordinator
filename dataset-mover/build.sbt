import Dependencies._

name := "dataset-mover"

libraryDependencies ++= Seq(
)

assembly/test := {}

assembly/mainClass := Some("com.socrata.datacoordinator.mover.Main")

publish / skip := true

run/fork := true

assemblyMergeStrategy  := {
  case PathList("module-info.class") => MergeStrategy.discard
  case x if x.endsWith("/module-info.class") => MergeStrategy.discard
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

excludeDependencies ++= Seq(
  ExclusionRule("log4j", "log4j")
)
