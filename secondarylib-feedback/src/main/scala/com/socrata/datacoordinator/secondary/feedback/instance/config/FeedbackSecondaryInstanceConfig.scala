package com.socrata.datacoordinator.secondary.feedback.instance.config

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.{ConfigUtil, Config}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry
import scala.collection.JavaConverters._

class FeedbackSecondaryInstanceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  // handle blank root
  override protected def path(key: String*): String = {
    val fullKey = if (root.isEmpty) key else ConfigUtil.splitPath(root).asScala ++ key
    ConfigUtil.joinPath(fullKey: _*)
  }

  val baseBatchSize = getInt("base-batch-size")
  val computationRetries = getInt("computation-retries")
  val internalMutationScriptRetries = getInt("internal-mutation-script-retries")
  val mutationScriptRetries = getInt("mutation-script-retries")
  val curator = getConfig("curator", new CuratorConfig(_, _))
  val dataCoordinatorService = getString("data-coordinator-service")

  val debugString = config.root.render()
}

class CuratorConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val ensemble = getStringList("ensemble").mkString(",")
  val namespace = getString("namespace")
  val sessionTimeout = getDuration("session-timeout")
  val connectTimeout = getDuration("connect-timeout")
  val baseRetryWait = getDuration("base-retry-wait")
  val maxRetryWait = getDuration("max-retry-wait")
  val maxRetries = getInt("max-retries")
  val serviceBasePath = getString("service-base-path")
}

object CuratorFromConfig {
  def apply(config: CuratorConfig) =
    CuratorFrameworkFactory.builder.
      connectString(config.ensemble).
      sessionTimeoutMs(config.sessionTimeout.toMillis.toInt).
      connectionTimeoutMs(config.connectTimeout.toMillis.toInt).
      retryPolicy(new retry.BoundedExponentialBackoffRetry(config.baseRetryWait.toMillis.toInt,
      config.maxRetryWait.toMillis.toInt,
      config.maxRetries)).
      namespace(config.namespace).
      build()
}
