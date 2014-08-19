resolvers ++= Seq(
  "socrata releases" at "http://repository-socrata-oss.forge.cloudbees.com/release",
  "DiversIT repo" at "http://repository-diversit.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.3.0")

libraryDependencies ++= Seq(
  "com.rojoma" %% "rojoma-json" % "2.4.3",
  "com.rojoma" %% "simple-arm" % "1.1.10"
)
