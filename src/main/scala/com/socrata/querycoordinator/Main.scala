package com.socrata.querycoordinator

import scala.concurrent.duration._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.PropertyConfigurator
import com.rojoma.simplearm.util._

import com.socrata.thirdparty.typesafeconfig.Propertizer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry
import org.apache.curator.x.discovery.{ServiceInstanceBuilder, ServiceDiscoveryBuilder, strategies}
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.SocrataServerJetty
import com.socrata.soql.types.SoQLAnalysisType
import com.socrata.soql.{AnalysisSerializer, SoQLAnalyzer}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.google.protobuf.CodedOutputStream
import java.util.concurrent.{ExecutorService, Executors}
import com.rojoma.simplearm.Resource
import com.rojoma.json.ast.JString
import com.socrata.http.client.{HttpClientHttpClient, InetLivenessChecker}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import java.net.{InetSocketAddress, InetAddress}
import com.socrata.querycoordinator.util.TeeToTempInputStream
import java.io.InputStream
import com.socrata.http.server.util.handlers.{ThreadRenamingHandler, LoggingHandler}

final abstract class Main

object Main extends App {
  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if(ifaces.isEmpty) config
    else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString("com.socrata.query-coordinator.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  val config = try {
    new QueryCoordinatorConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.query-coordinator")
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  def typeSerializer(typ: SoQLAnalysisType) = typ.canonical.name.name
  val analysisSerializer = new AnalysisSerializer[String, SoQLAnalysisType](identity, typeSerializer)

  implicit object executorResource extends Resource[ExecutorService]{
    def close(a: ExecutorService) { a.shutdown() }
  }

  for {
    executor <- managed(Executors.newFixedThreadPool(5))
    pingProvider <- managed(new InetLivenessChecker(5.seconds, 1.second, 5, executor))
    httpClient <- managed(new HttpClientHttpClient(pingProvider, executor, userAgent = "Query Coordinator"))
    curator <- managed(CuratorFrameworkFactory.builder.
      connectString(config.curator.ensemble).
      sessionTimeoutMs(config.curator.sessionTimeout.toMillis.toInt).
      connectionTimeoutMs(config.curator.connectTimeout.toMillis.toInt).
      retryPolicy(new retry.BoundedExponentialBackoffRetry(config.curator.baseRetryWait.toMillis.toInt,
                                                           config.curator.maxRetryWait.toMillis.toInt,
                                                           config.curator.maxRetries)).
      namespace(config.curator.namespace).
      build())
    discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[AuxiliaryData]).
      client(curator).
      basePath(config.advertisement.basePath).
      build())
    dataCoordinatorProviderProvider <- managed(new ServiceProviderProvider(
      discovery,
      new strategies.RoundRobinStrategy))
    pongProvider <- managed(new LivenessCheckResponder(new InetSocketAddress(InetAddress.getByName(config.advertisement.address), 0)))
  } {
    curator.start()
    discovery.start()
    pingProvider.start()
    pongProvider.start()

    def teeStream(in: InputStream) = new TeeToTempInputStream(in)

    val secondaryInstanceSelector = new SecondaryInstanceSelector(config)

    val handler = new Service(
      dataCoordinatorProviderProvider,
      new SchemaFetcher(httpClient),
      new QueryParser(analyzer, config.maxRows, config.defaultRowsLimit),
      new QueryExecutor(httpClient, analysisSerializer, teeStream),
      config.connectTimeout,
      config.schemaTimeout,
      config.queryTimeout,
      (_, _, _) => (),
      (_, _) => None,
      secondaryInstanceSelector,
      new QueryRewriter(analyzer),
      new RollupInfoFetcher(httpClient))

    val auxData = new AuxiliaryData(Some(pongProvider.livenessCheckInfo))

    val serv = new SocrataServerJetty(
      handler = ThreadRenamingHandler(LoggingHandler(handler)),
      port = config.network.port,
      broker = new CuratorBroker(discovery, config.advertisement.address, config.advertisement.name, Some(auxData))
    )
    log.info("Ready to go!  kicking off the server...")
    serv.run()
  }

  log.info("Terminated normally")
}
