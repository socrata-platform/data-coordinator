package com.socrata.datacoordinator.secondary.messaging.eurybates

import java.util.{Properties, UUID}
import java.util.concurrent.Executor
import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.datacoordinator.secondary.messaging.{Message, MessageProducer, NoOpMessageProducer}
import com.socrata.eurybates
import com.socrata.eurybates.activemq.ActiveMQServiceProducer
import com.socrata.eurybates.zookeeper.ServiceConfiguration
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.zookeeper.ZooKeeperProvider
import com.typesafe.config.{Config, ConfigException}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.transport.TransportListener
import org.slf4j.LoggerFactory

import java.io.IOException

class EurybatesMessageProducer(sourceId: String,
                        zkp: ZooKeeperProvider,
                        executor: Executor,
                        connectionString: String,
                        user: Option[String],
                        password: Option[String]) extends MessageProducer {
  val log = LoggerFactory.getLogger(classOf[EurybatesMessageProducer])

  log.info(s"Initializing eurybates message producer with connection string: ${connectionString} with username: ${user.isDefined} and password: ${password.isDefined}")
  val monitor = new ConnectionStateMonitor
  val connFactory = new ActiveMQConnectionFactory(connectionString)
  connFactory.setTransportListener(monitor)
  user.foreach(connFactory.setUserName)
  password.foreach(connFactory.setPassword)
  val connection = connFactory.createConnection

  private val producer = new ActiveMQServiceProducer(connection, sourceId, false, false)
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
  val activemqConnStr: String
  val activemqUser: Option[String]
  val activemqPassword: Option[String]
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
  val log = LoggerFactory.getLogger(classOf[EurybatesConfigImpl])

  val activemqConfig = getRawConfig("activemq")
  override val activemqConnStr = activemqConfig.getString("connection-string")
  override val activemqUser: Option[String] = maybeGetString(activemqConfig, "user")
  override val activemqPassword: Option[String] = maybeGetString(activemqConfig, "password")

  def maybeGetString(config: Config, key: String): Option[String] = {
    try {
      val value = config.getString(key)
      Some(value)
    } catch {
      case e: ConfigException.Missing => {
        log.info(s"ActiveMQ ${key} not specified in configuration")
        None
      }
    }
  }
}

class ZookeeperConfigImpl(config: Config, root: String) extends ConfigClass(config, root) with ZookeeperConfig {
  override val connSpec = getString("conn-spec")
  override val sessionTimeout = getDuration("session-timeout").toMillis.toInt
}

object MessageProducerFromConfig {
  def apply(watcherId: UUID, executor: Executor, config: Option[MessageProducerConfig]): MessageProducer = config match {
    case Some(conf) =>
      val zkp = new ZooKeeperProvider(conf.zookeeper.connSpec, conf.zookeeper.sessionTimeout, executor)
      new EurybatesMessageProducer(watcherId.toString, zkp, executor, conf.eurybates.activemqConnStr, conf.eurybates.activemqUser, conf.eurybates.activemqPassword)
    case None => NoOpMessageProducer
  }
}

class ConnectionStateMonitor extends TransportListener {
  val logger = LoggerFactory.getLogger(getClass)
  var connected = false

  override def onCommand(command: Object): Unit = {}
  override def onException(exception: IOException): Unit = {}
  override def transportInterupted(): Unit = connected = false
  override def transportResumed(): Unit = connected = true
}
