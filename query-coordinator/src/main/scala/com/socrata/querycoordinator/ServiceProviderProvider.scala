package com.socrata.querycoordinator

import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap

import org.apache.curator.x.discovery.{ProviderStrategy, ServiceDiscovery, ServiceProvider}

import scala.collection.JavaConverters._

class ServiceProviderProvider[T](serviceDiscovery: ServiceDiscovery[T],
                                 providerStrategy: ProviderStrategy[T]) extends Closeable {
  private val providers = new ConcurrentHashMap[String, ServiceProvider[T]]
  private var closed = false

  private def maybeCreateProvider(instance: String): ServiceProvider[T] = synchronized {
    if(closed) throw new IllegalStateException("ServiceProviderProvider closed")
    Option(providers.get(instance)) match {
      case None =>
        val provider = serviceDiscovery.serviceProviderBuilder.
          providerStrategy(providerStrategy).
          serviceName(instance).
          build()
        provider.start()

        providers.put(instance, provider)
        provider
      case Some(p) =>
        p
    }
  }

  def provider(instance: String): ServiceProvider[T] = {
    Option(providers.get(instance)) match {
      case Some(p) => p
      case None => maybeCreateProvider(instance)
    }
  }

  def close(): Unit = synchronized {
    providers.values.asScala.foreach(_.close())
    providers.clear()
    closed = true
  }
}
