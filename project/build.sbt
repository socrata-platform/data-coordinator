resolvers ++= Seq(
  "socrata releases" at "http://repository-socrata-oss.forge.cloudbees.com/release",
  "DiversIT repo" at "http://repository-diversit.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.3.0")

lazy val root = project.in(file(".")).dependsOn(scalabuff)

lazy val scalabuff = uri("git://github.com/tkawachi/sbt-scalabuff.git#47a1ab48a50ccd42c7c5a44d8ff605184e2b3707")

libraryDependencies ++= Seq(
  "com.rojoma" %% "rojoma-json" % "2.4.3",
  "com.rojoma" %% "simple-arm" % "1.2.0"
)
