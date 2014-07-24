import AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy

com.socrata.cloudbeessbt.SocrataCloudbeesSbt.socrataSettings(assembly = true)

name := "query-coordinator"

scalaVersion := "2.10.4"

resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"

libraryDependencies ++= Seq(
  "com.socrata" %% "soql-stdlib" % "[0.2.1,1.0.0)",
  "com.socrata" %% "socrata-thirdparty-utils" % "[2.1.0,3.0.0)",
  "com.typesafe" % "config" % "1.0.0",
  "com.socrata" %% "socrata-http-curator-broker" % "[2.0.0,3.0.0)" exclude ("org.jboss.netty", "netty" /* see ZOOKEEPER-1681 */),
  "com.socrata" %% "socrata-http-client" % "[2.0.0,3.0.0)",
  "com.google.protobuf" % "protobuf-java" % "2.4.1",
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
  "com.typesafe" %% "scalalogging-slf4j" % "1.1.0",
  "com.rojoma" %% "rojoma-json" % "[2.4.3,3.0.0)"
)

scalacOptions ++= Seq("-deprecation", "-feature")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { old =>
  {
    case "about.html" => MergeStrategy.rename
    case x => old(x)
  }
}

resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, scalaVersion in Compile, version in Compile) map { (resourceManaged, name, scalaVersion, version) =>
  import com.rojoma.simplearm.util._
  import com.rojoma.json.util.JsonUtil._
  val file = resourceManaged / "query-coordinator-version.json"
  val revision = Process(Seq("git", "describe", "--always", "--dirty", "--long")).!!.split("\n")(0)
  val result = Map(
    "service" -> name,
    "version" -> version,
    "revision" -> revision,
    "scala" -> scalaVersion
  ) ++ sys.env.get("BUILD_TAG").map("build" -> _)
  resourceManaged.mkdirs()
  for {
    stream <- managed(new java.io.FileOutputStream(file))
    w <- managed(new java.io.OutputStreamWriter(stream, "UTF-8"))
  } {
    writeJson(w, result, pretty = true)
    w.write("\n")
  }
  Seq(file)
}
