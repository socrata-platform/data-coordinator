package com.socrata.datacoordinator.secondary

import java.util.{Properties, UUID}
import java.util.concurrent.Executor

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.socrata.eurybates
import com.socrata.eurybates.ServiceName
import com.socrata.eurybates.zookeeper.ServiceConfiguration
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.zookeeper.ZooKeeperProvider
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class Producer(sourceId: String, instance: String, zkp: ZooKeeperProvider, executor: Executor, properties: Properties) {
  val log = LoggerFactory.getLogger(classOf[Producer])

  private val producer = eurybates.Producer(sourceId, properties)
  private val serviceConfiguration = new ServiceConfiguration(zkp, executor, setServiceNames)

  def start(): Unit = {
    producer.start()
    serviceConfiguration.start()
  }

  def setServiceNames(serviceNames: Set[ServiceName]): Unit = {
    producer.setServiceNames(serviceNames)
  }

  def shutdown(): Unit = {
    producer.stop()
  }

  def send(message: String => ProducerMessage): Unit = {
    val m = message(instance)
    try {
      producer.send(eurybates.Message(ProducerMessage.tag(m), JsonEncode.toJValue(m)))
    } catch {
      case e: IllegalStateException =>
        log.error("Failed to send message! Are there no open sessions to queues?", e)
    }
  }
}

class ProducerConfig(config: Config, root: String) extends ConfigClass(config, root) {
  def p(path: String) = root + "." + path
  val eurybates = new EurybatesConfig(config, p("eurybates"))
  val zookeeper = new ZookeeperConfig(config, p("zookeeper"))
}

class EurybatesConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val producers = getString("producers")
  val activemqConnStr = getRawConfig("activemq").getString("connection-string")
}

class ZookeeperConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val connSpec = getString("conn-spec")
  val sessionTimeout = getDuration("session-timeout").toMillis.toInt
}

object ProducerFromConfig {
  def apply(watcherId: UUID, instance: String, executor: Executor, config: ProducerConfig): Producer = {
    val properties = new Properties()
    properties.setProperty("eurybates.producers", config.eurybates.producers)
    properties.setProperty("eurybates.activemq.connection_string", config.eurybates.activemqConnStr)
    val zkp = new ZooKeeperProvider(config.zookeeper.connSpec, config.zookeeper.sessionTimeout, executor)
    new Producer(watcherId.toString, instance, zkp, executor, properties)
  }
}
