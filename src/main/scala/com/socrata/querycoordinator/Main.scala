package com.socrata.querycoordinator

import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import com.rojoma.simplearm.util._

import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder
import com.netflix.curator.x.discovery.strategies
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.SocrataServerJetty
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig}
import com.ning.http.client.providers.grizzly.{GrizzlyAsyncHttpProvider, GrizzlyAsyncHttpProviderConfig}
import com.socrata.soql.types.{SoQLVersion, SoQLID, SoQLTextLiteral, SoQLAnalysisType}
import com.socrata.soql.{AnalysisSerializer, SoQLAnalyzer}
import com.socrata.soql.functions.{SoQLFunctions, SoQLFunctionInfo, SoQLTypeInfo}
import com.google.protobuf.CodedOutputStream
import com.ibm.icu.util.CaseInsensitiveString

final abstract class Main

object evidences {
  implicit def httpResource[A <: dispatch.Http] = new com.rojoma.simplearm.Resource[A] {
    def close(a: A) { a.shutdown() }
  }
}

object SoQLFunctionInfoWithIds extends SoQLFunctionInfo {
  private val quad = "(?:[2-9a-kmnp-z]{4})"
  private val punct = "[-._~]"

  object IdStringRep {
    private val RowIdentifierPattern = ("row-" + quad + punct + quad + punct + quad).r
    def unapply(text: String): Boolean =
      RowIdentifierPattern.pattern.matcher(text).matches()
    def unapply(text: CaseInsensitiveString): Boolean = unapply(text.getString)
  }

  object VersionStringRep {
    private val RowVersionPattern = ("rv-" + quad + punct + quad + punct + quad).r
    def unapply(text: String): Boolean =
      RowVersionPattern.pattern.matcher(text).matches()
    def unapply(text: CaseInsensitiveString): Boolean = unapply(text.getString)
  }

  override def implicitConversions(from: SoQLAnalysisType, to: SoQLAnalysisType) = (from,to) match {
    case (SoQLTextLiteral(IdStringRep()), SoQLID) =>
      Some(SoQLFunctions.TextToRowIdentifier.monomorphic.getOrElse(sys.error("Text to row identifier is not monomorphic?")))
    case (SoQLTextLiteral(VersionStringRep()), SoQLVersion) =>
      Some(SoQLFunctions.TextToRowVersion.monomorphic.getOrElse(sys.error("Text to row version is not monomorphic?")))
    case _ =>
      super.implicitConversions(from, to)
  }
}

object Main extends App {
  import evidences._

  val config = try {
    new QueryCoordinatorConfig(ConfigFactory.load().getConfig("com.socrata.query-coordinator"))
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfoWithIds)
  def typeSerializer(out: CodedOutputStream, typ: SoQLAnalysisType) {
    out.writeStringNoTag(typ.canonical.name.name)
  }
  val analysisSerializer = new AnalysisSerializer[SoQLAnalysisType](typeSerializer)

  for {
    dispatchHttp <- managed(dispatch.Http)
    http <- managed(dispatchHttp.configure { builder =>
      builder.setMaxRequestRetry(0)
    })
    curator <- managed(CuratorFrameworkFactory.builder.
      connectString(config.curator.ensemble).
      sessionTimeoutMs(config.curator.sessionTimeout.toMillis.toInt).
      connectionTimeoutMs(config.curator.connectTimeout.toMillis.toInt).
      retryPolicy(new retry.BoundedExponentialBackoffRetry(config.curator.baseRetryWait.toMillis.toInt,
                                                           config.curator.maxRetryWait.toMillis.toInt,
                                                           config.curator.maxRetries)).
      namespace(config.curator.namespace).
      build())
    discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[Void]).
      client(curator).
      basePath(config.advertisement.basePath).
      build())
    dataCoordinatorProviderProvider <- managed(new ServiceProviderProvider(
      discovery,
      new strategies.RoundRobinStrategy,
      "data-coordinator"))
  } {
    curator.start()
    discovery.start()

    val handler = new Service(
      http,
      dataCoordinatorProviderProvider,
      config.schemaTimeout,
      config.initialResponseTimeout,
      config.responseDataTimeout,
      analyzer,
      analysisSerializer,
      (_, _) => (),
      _ => None)

    val serv = new SocrataServerJetty(
      handler = handler,
      port = config.network.port,
      broker = new CuratorBroker(discovery, config.advertisement.address, config.advertisement.name)
    )
    log.info("Ready to go!  kicking off the server...")
    serv.run()
  }

  log.info("Terminated normally")
}
