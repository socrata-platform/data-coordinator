package com.socrata.querycoordinator

import java.io.InputStream
import java.util.concurrent.{ExecutorService, Executors}

import com.rojoma.json.ast.JString
import com.rojoma.simplearm.Resource
import com.rojoma.simplearm.util._
import com.socrata.http.client.HttpClientHttpClient
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.SocrataServerJetty
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.http.server.util.handlers.{NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.querycoordinator.util.TeeToTempInputStream
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.SoQLAnalysisType
import com.socrata.soql.{AnalysisSerializer, SoQLAnalyzer}
import com.socrata.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.thirdparty.metrics.{MetricsReporter, SocrataHttpSupport}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.x.discovery.{ServiceInstanceBuilder, strategies}
import org.apache.log4j.PropertyConfigurator

final abstract class Main

object Main extends App {

  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if (ifaces.isEmpty) {
      config
    } else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig =
        ConfigFactory.parseString("com.socrata.query-coordinator.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  val config = try {
    new QueryCoordinatorConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.query-coordinator")
  } catch {
    case e: Exception =>
      print(e.toString)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  def typeSerializer(typ: SoQLAnalysisType): String = typ.canonical.name.name
  val analysisSerializer = new AnalysisSerializer[String, SoQLAnalysisType](identity, typeSerializer)

  implicit object executorResource extends Resource[ExecutorService]{
    def close(a: ExecutorService): Unit = { a.shutdown() }
  }

  val threadPoolSize = 5
  for {
    executor <- managed(Executors.newFixedThreadPool(threadPoolSize))
    httpClientConfig <- unmanaged(HttpClientHttpClient.defaultOptions.withUserAgent("Query Coordinator"))
    httpClient <- managed(new HttpClientHttpClient(executor, httpClientConfig))
    curator <- CuratorFromConfig(config.curator)
    discovery <- DiscoveryFromConfig(classOf[AuxiliaryData], curator, config.discovery)
    dataCoordinatorProviderProvider <- managed(new ServiceProviderProvider(
      discovery,
      new strategies.RoundRobinStrategy))
    pongProvider <- managed(new LivenessCheckResponder(config.livenessCheck))
    reporter <- MetricsReporter.managed(config.metrics)
  } {
    pongProvider.start()

    def teeStream(in: InputStream): TeeToTempInputStream = new TeeToTempInputStream(in)

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

    val logOptions = NewLoggingHandler.defaultOptions.copy(
                       logRequestHeaders = Set(ReqIdHeader, "X-Socrata-Resource"))

    val broker = new CuratorBroker(discovery, config.discovery.address, config.discovery.name,
                                   Some(auxData))

    val serv = new SocrataServerJetty(
      ThreadRenamingHandler(NewLoggingHandler(logOptions)(handler)),
      SocrataServerJetty.defaultOptions.
                         withPort(config.network.port).
                         withExtraHandlers(List(SocrataHttpSupport.getHandler(config.metrics))).
                         withPoolOptions(SocrataServerJetty.Pool(config.threadpool)).
                         withBroker(broker)
    )
    log.info("Ready to go!  kicking off the server - downstream query servers {}...",
             config.allSecondaryInstanceNames.mkString("[", ", ", "]"))
    serv.run()
  }

  log.info("Terminated normally")
}
