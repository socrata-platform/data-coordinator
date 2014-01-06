resolvers ++= Seq(
  "socrata releases" at "http://repository-socrata-oss.forge.cloudbees.com/release",
  "DiversIT repo" at "http://repository-diversit.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.1.1")

addSbtPlugin("com.github.sbt" %% "sbt-scalabuff" % "0.2")

libraryDependencies ++= Seq(
  "com.rojoma" %% "rojoma-json" % "2.0.0",
  "com.rojoma" %% "simple-arm" % "1.2.0"
)
