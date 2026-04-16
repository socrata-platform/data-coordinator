resolvers ++= Seq(
  Resolver.url("socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.8")

/* 
  sbt-protoc:1.0.8 calls for sbt-platform-deps:1.0.1,
  but 1.0.2 is the only version currently available in mavenCentral.
  manually use 1.0.2 until sbt-protoc is updated. 

  see https://github.com/thesamet/sbt-protoc/pull/334
 */
addSbtPlugin("org.portable-scala" % "sbt-platform-deps" % "1.0.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

libraryDependencies ++= Seq(
  "com.rojoma" %% "rojoma-json-v3" % "3.9.1",
  "com.rojoma" %% "simple-arm-v2" % "2.1.0",
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.20"
)
