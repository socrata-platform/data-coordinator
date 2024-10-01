import Dependencies._

name := "dataset-mover"

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.46.0.0"
)

assembly/test := {}

assembly/mainClass := Some("com.socrata.datacoordinator.mover.Main")

publish / skip := true

run/fork := true

