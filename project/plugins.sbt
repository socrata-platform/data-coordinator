resolvers ++= Seq(
  Resolver.url("socrata", url("https://repo.socrata.com/artifactor/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.6.8")

lazy val root = project.in(file(".")).dependsOn(scalabuff)

lazy val scalabuff = uri("git://github.com/tkawachi/sbt-scalabuff.git#47a1ab48a50ccd42c7c5a44d8ff605184e2b3707")

libraryDependencies ++= Seq(
  "com.rojoma" %% "rojoma-json-v3" % "3.9.1",
  "com.rojoma" %% "simple-arm-v2" % "2.1.0"
)
