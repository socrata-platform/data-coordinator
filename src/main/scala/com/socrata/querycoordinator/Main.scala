package com.socrata.querycoordinator

import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import com.rojoma.simplearm.util._

import com.socrata.util.config.Propertizer
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.SocrataServerJetty

final abstract class Main

object Main extends App {
  val config = try {
    new QueryCoordinatorConfig(ConfigFactory.load().getConfig("com.socrata.query-coordinator"))
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  for {
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
  } {
    curator.start()
    discovery.start()
    val serv = new SocrataServerJetty(
      handler = new Service,
      port = config.network.port,
      broker = new CuratorBroker(discovery, config.advertisement.address, config.advertisement.name)
    )
    log.info("Ready to go!  kicking off the server...")
    serv.run()
  }

  log.info("Terminated normally")
}
