import Dependencies._

name := "dataset-mover"

libraryDependencies ++= Seq(
)

assembly/test := {}

assembly/mainClass := Some("com.socrata.datacoordinator.mover.Main")

publish / skip := true

run/fork := true

