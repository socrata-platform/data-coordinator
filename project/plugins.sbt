resolvers ++= Seq(
  "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.5.3")

lazy val root = project.in(file(".")).dependsOn(scalabuff)

lazy val scalabuff = uri("git://github.com/tkawachi/sbt-scalabuff.git#47a1ab48a50ccd42c7c5a44d8ff605184e2b3707")

libraryDependencies ++= Seq(
  "com.rojoma" %% "rojoma-json-v3" % "3.2.2",
  "com.rojoma" %% "simple-arm" % "1.2.0"
)
