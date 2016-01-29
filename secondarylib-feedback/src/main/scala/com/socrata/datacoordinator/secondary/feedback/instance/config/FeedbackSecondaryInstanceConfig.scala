package com.socrata.datacoordinator.secondary.feedback.instance.config

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry

class FeedbackSecondaryInstanceConfig(config: Config, instanceName: String) extends ConfigClass(config, instanceName) {
  val baseBatchSize = getInt("base-batch-size")
  val mutationScriptRetries = getInt("mutation-script-retries")
  val computationRetires = getInt("computation-retries")
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
