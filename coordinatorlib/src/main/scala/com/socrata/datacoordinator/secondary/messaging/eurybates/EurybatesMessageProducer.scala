package com.socrata.datacoordinator.secondary.messaging.eurybates

import java.util.{Properties, UUID}
import java.util.concurrent.Executor
import scala.concurrent.duration.FiniteDuration

import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.datacoordinator.secondary.messaging.{NoOpMessageProducer, Message, MessageProducer}
import com.socrata.eurybates
import com.socrata.eurybates.zookeeper.ServiceConfiguration
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.zookeeper.ZooKeeperProvider
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class EurybatesMessageProducer(sourceId: String,
                        zkp: ZooKeeperProvider,
                        executor: Executor,
                        properties: Properties) extends MessageProducer {
  val log = LoggerFactory.getLogger(classOf[EurybatesMessageProducer])

  private val producer = eurybates.Producer(sourceId, properties)
  private val serviceConfiguration = new ServiceConfiguration(zkp, executor, setServiceNames)

  def start(): Unit = {
    producer.start()
    serviceConfiguration.start()
  }

  def setServiceNames(serviceNames: Set[String]): Unit = {
    producer.setServiceNames(serviceNames)
  }

  def shutdown(): Unit = {
    producer.stop()
  }

  def send(message: Message): Unit = {
    log.debug("Sending message: {}", message)
    try {
      producer.send(eurybates.Message(EurybatesMessage.tag(message), JsonEncode.toJValue(message)))
    } catch {
      case e: IllegalStateException =>
        log.error("Failed to send message! Are there no open sessions to queues?", e)
    }
  }
}

trait MessageProducerConfig {
  val eurybates: EurybatesConfig
  val zookeeper: ZookeeperConfig
}

trait EurybatesConfig {
  val producers: String
  val activemqConnStr: String
}

trait ZookeeperConfig {
  val connSpec: String
  val sessionTimeout: Int
}


class MessageProducerConfigImpl(config: Config, root: String) extends ConfigClass(config, root) with MessageProducerConfig {
  def p(path: String) = root + "." + path // TODO: something better?
  override val eurybates = new EurybatesConfigImpl(config, p("eurybates"))
  override val zookeeper = new ZookeeperConfigImpl(config, p("zookeeper"))
}

class EurybatesConfigImpl(config: Config, root: String) extends ConfigClass(config, root) with EurybatesConfig {
  override val producers = getString("producers")
  override val activemqConnStr = getRawConfig("activemq").getString("connection-string")
}

class ZookeeperConfigImpl(config: Config, root: String) extends ConfigClass(config, root) with ZookeeperConfig {
  override val connSpec = getString("conn-spec")
  override val sessionTimeout = getDuration("session-timeout").toMillis.toInt
}

object MessageProducerFromConfig {
  def apply(watcherId: UUID, executor: Executor, config: Option[MessageProducerConfig]): MessageProducer = config match {
    case Some(conf) =>
      val properties = new Properties()
      properties.setProperty("eurybates.producers", conf.eurybates.producers)
      properties.setProperty("eurybates.activemq.connection_string", conf.eurybates.activemqConnStr)
      val zkp = new ZooKeeperProvider(conf.zookeeper.connSpec, conf.zookeeper.sessionTimeout, executor)
      new EurybatesMessageProducer(watcherId.toString, zkp, executor, properties)
    case None => NoOpMessageProducer
  }
}
