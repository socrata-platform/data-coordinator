seq(socrataSettings(): _*)

name := "dataset-coordinator"

version := "0.0.1"

scalaVersion := "2.9.2"

libraryDependencies ++= Seq(
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
  "com.socrata" %% "socrata-id" % "2.0.0",
  "com.rojoma" %% "rojoma-json" % "2.0.0",
  "com.rojoma" %% "simple-arm" % "1.1.10",
  "com.h2database" % "h2" % "1.3.166" % "test"
)
