package com.socrata.datacoordinator.mover

import scala.collection.JavaConverters._

import com.socrata.datacoordinator.secondary.config.SecondaryConfig
import com.socrata.datacoordinator.common.collocation.CollocationConfig
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.http.server.livenesscheck.LivenessCheckConfig
import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.typesafe.config.{Config, ConfigUtil}
import java.util.concurrent.TimeUnit

import com.socrata.datacoordinator.service.ReportsConfig

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.thirdparty.metrics.MetricsOptions

class MoverConfig(val config: Config, root: String, hostPort: Int => Int) extends ConfigClass(config, root) {
  val truths = locally {
    val secondaries = getRawConfig("truths")
    secondaries.root.entrySet.iterator.asScala.foldLeft(Map.empty[String, DataSourceConfig]) { (acc, ent) =>
      acc + (ent.getKey -> new DataSourceConfig(config, root + "." + ConfigUtil.joinPath("truths", ent.getKey)))
    }
  }
  val secondaries = locally {
    val secondaries = getRawConfig("secondaries")
    secondaries.root.entrySet.iterator.asScala.foldLeft(Map.empty[String, DataSourceConfig]) { (acc, ent) =>
      acc + (ent.getKey -> new DataSourceConfig(config, root + "." + ConfigUtil.joinPath("secondaries", ent.getKey)))
    }
  }
  val sodaFountain = getConfig("soda-fountain", new DataSourceConfig(_, _))

  val logProperties = getRawConfig("log4j")
  val tablespace = getString("tablespace")
  val writeLockTimeout = getDuration("write-lock-timeout")
  val acceptableSecondaries = getStringList("acceptable-secondaries").toSet
}
