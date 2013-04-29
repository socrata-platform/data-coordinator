package com.socrata.querycoordinator

import java.io.Closeable
import com.netflix.curator.x.discovery.{ProviderStrategy, ServiceProvider, ServiceDiscovery}
import scala.collection.concurrent

class ServiceProviderProvider[T](serviceDiscovery: ServiceDiscovery[T], providerStrategy: ProviderStrategy[T], baseName: String) extends Closeable {
  private val providers = new concurrent.TrieMap[Int, ServiceProvider[T]]

  private def maybeCreateProvider(n: Int): ServiceProvider[T] = synchronized {
    providers.get(n) match {
      case None =>
        val provider = serviceDiscovery.serviceProviderBuilder.
          providerStrategy(providerStrategy).
          serviceName(baseName + "." + n).
          build()
        provider.start()
        providers(n) = provider
        provider
      case Some(p) =>
        p
    }
  }

  def provider(n: Int): ServiceProvider[T] = {
    providers.get(n) match {
      case Some(p) => p
      case None =>
        maybeCreateProvider(n)
    }
  }

  def close() = synchronized {
    providers.values.foreach(_.close())
    providers.clear()
  }
}
