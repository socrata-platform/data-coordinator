package com.socrata.datacoordinator.secondary.feedback.instance

import java.util.concurrent.Executors

import com.rojoma.simplearm.v2.{Resource, ResourceScope}
import com.socrata.datacoordinator.secondary.feedback.instance.config.{CuratorFromConfig, FeedbackSecondaryInstanceConfig}
import com.socrata.datacoordinator.secondary.feedback.monitor.{DummyStatusMonitor, StatusMonitor}
import com.socrata.datacoordinator.secondary.feedback.{RowComputeInfo, FeedbackSecondary}
import com.socrata.http.client.{HttpClientHttpClient, HttpClient}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import org.apache.curator.x.discovery.{strategies, ServiceDiscoveryBuilder}

abstract class FeedbackSecondaryInstance[RCI <: RowComputeInfo[SoQLValue]](config: FeedbackSecondaryInstanceConfig) extends FeedbackSecondary[SoQLType, SoQLValue, RCI] {

  log.info("Configuration:\n" + config.debugString)

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

  protected val discovery = res(ServiceDiscoveryBuilder.builder(classOf[Void]).
    client(curator).
    basePath(config.curator.namespace). // com.socrata/soda
    build())
  guarded(discovery.start())

  protected val provider = res(discovery.serviceProviderBuilder().
    providerStrategy(new strategies.RoundRobinStrategy).
    serviceName(config.dataCoordinatorService). // data-coordinator
    build())
  guarded(provider.start())

  override val httpClient: HttpClient = res(new HttpClientHttpClient(executor))

  override def hostAndPort(): (String, Int) = {
    val instance = provider.getInstance()
    (instance.getAddress, instance.getPort)
  }

  override val baseBatchSize: Int = config.baseBatchSize

  override val mutationScriptRetries: Int = config.mutationScriptRetries

  override val repFor = SoQLValueRep
  override val typeFor = SoQLValueFor

  override val statusMonitor: StatusMonitor = new DummyStatusMonitor // TODO: replace with status monitor connected to ISS

  override def shutdown(): Unit = {
    resourceScope.close()
  }
}
