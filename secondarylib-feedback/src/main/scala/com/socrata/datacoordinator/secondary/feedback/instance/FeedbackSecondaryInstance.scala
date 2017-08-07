package com.socrata.datacoordinator.secondary.feedback.instance

import java.util.concurrent.Executors

import com.rojoma.json.v3.ast.JValue
import com.rojoma.simplearm.v2.{Resource, ResourceScope}
import com.socrata.curator.ProviderCache
import com.socrata.datacoordinator.secondary.feedback.instance.config.{CuratorFromConfig, FeedbackSecondaryInstanceConfig}
import com.socrata.datacoordinator.secondary.feedback.monitor.{DummyStatusMonitor, StatusMonitor}
import com.socrata.datacoordinator.secondary.feedback.{HttpDataCoordinatorClient, DataCoordinatorClient, FeedbackSecondary}
import com.socrata.http.client.{HttpClientHttpClient, HttpClient}
import com.socrata.http.common.AuxiliaryData
import com.socrata.soql.types.{SoQLType, SoQLValue}
import org.apache.curator.x.discovery.{strategies, ServiceDiscoveryBuilder}

abstract class FeedbackSecondaryInstance(config: FeedbackSecondaryInstanceConfig) extends FeedbackSecondary[SoQLType, SoQLValue] {

  log.debug("Configuration:\n" + config.debugString)

  private val resourceScope = new ResourceScope("feedback secondary")

  protected def guarded[T](f: => T) =
    try { f }
    catch { case t: Throwable =>
      try {
        resourceScope.close()
      } catch {
        case t2: Throwable =>
          t.addSuppressed(t2)
      }
      throw t
    }

  protected def res[T : Resource](f : => T) = guarded(resourceScope.open(f))

  protected implicit def executorShutdownPolicy = Resource.executorShutdownNoTimeout

  private val executor = res(Executors.newCachedThreadPool())

  protected val curator = res(CuratorFromConfig(config.curator))
  guarded(curator.start())

  protected val discovery = res(ServiceDiscoveryBuilder.builder(classOf[AuxiliaryData]).
    client(curator).
    basePath(config.curator.serviceBasePath).
    build())
  guarded(discovery.start())

  protected val provider = res(new ProviderCache(
    discovery,
    new strategies.RoundRobinStrategy,
    config.dataCoordinatorService
  ))

  protected val httpClient: HttpClient = res(new HttpClientHttpClient(executor))

  protected def hostAndPort(instanceName: String): Option[(String, Int)] = {
    Option(provider(instanceName).getInstance()).map[(String, Int)](instance => (instance.getAddress, instance.getPort))
  }

  protected val internalDataCoordinatorRetryLimit: Int = config.internalDataCoordinatorRetries

  override def dataCoordinator = HttpDataCoordinatorClient(httpClient, hostAndPort, internalDataCoordinatorRetryLimit, typeFromJValue)

  override val baseBatchSize: Int = config.baseBatchSize

  override val dataCoordinatorRetryLimit: Int = config.dataCoordinatorRetries

  override val repFor = SoQLValueRepFor
  override val repFrom = SoQLValueRepFrom

  override val typeFor = SoQLTypeFor
  override val typeFromJValue = SoQLTypeFromJValue

  override val statusMonitor: StatusMonitor = new DummyStatusMonitor // TODO: replace with status monitor connected to ISS

  override def shutdown(): Unit = {
    resourceScope.close()
  }
}
