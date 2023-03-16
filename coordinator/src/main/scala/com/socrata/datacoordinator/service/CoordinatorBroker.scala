package com.socrata.datacoordinator.service

import scala.collection.JavaConverters._

import java.util.IdentityHashMap
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.curator.x.discovery.{ServiceDiscovery, ServiceInstance}
import org.slf4j.LoggerFactory
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AllowMissing}

import com.socrata.http.common.AuxiliaryData
import com.socrata.http.common.livenesscheck.LivenessCheckInfo
import com.socrata.http.server.ServerBroker
import com.socrata.http.server.curator.CuratorBroker

class CoordinatorBroker(
  portMapper: Int => Int,
  discovery: ServiceDiscovery[AuxiliaryData],
  address: String,
  serviceName: String,
  baseLCI: LivenessCheckInfo
) extends ServerBroker {
  private val log = LoggerFactory.getLogger(classOf[CoordinatorBroker])

  private val remappedLCI = new LivenessCheckInfo(portMapper(baseLCI.port), baseLCI.response)

  class WrappedCookie private[CoordinatorBroker] (
    private[CoordinatorBroker] val port: Int,
    private[CoordinatorBroker] val base: ServiceInstance[AuxiliaryData],
    private[CoordinatorBroker] var mutation: Option[ServiceInstance[AuxiliaryData]]
  )

  type Cookie = WrappedCookie

  private class TrueBroker(suffix: String) extends CuratorBroker[AuxiliaryData](
    discovery,
    address,
    serviceName + suffix,
    Some(new AuxiliaryData(Some(remappedLCI)))
  ) {
    override def register(port: Int) =
      super.register(portMapper(port))
  }

  private val baseBroker = new TrueBroker("")
  private val mutationBroker = new TrueBroker(".mutation")
  private val mutationEnabled = new AtomicBoolean(true)
  private val issuedCookies = new IdentityHashMap[WrappedCookie, Unit]

  override def register(port: Int): WrappedCookie = synchronized {
    val cookie = new WrappedCookie(
      port,
      baseBroker.register(port),
      if(mutationEnabled.get) Some(mutationBroker.register(port)) else None
    )
    issuedCookies.put(cookie, ())
    cookie
  }

  override def deregister(cookie: WrappedCookie) = synchronized {
    issuedCookies.remove(cookie)
    baseBroker.deregister(cookie.base)
    cookie.mutation.foreach(mutationBroker.deregister _)
  }

  // returns the old value
  def allowMutation(enable: Boolean): Boolean = {
    if(mutationEnabled.get == enable) {
      enable
    } else {
      synchronized {
        if(mutationEnabled.getAndSet(enable) == enable) return enable

        if(!enable) {
          log.warn("Disabling mutation registration")
        } else {
          log.warn("Re-enabling mutation registration")
        }
        for(cookie <- issuedCookies.keySet.asScala) {
          if(enable) {
            cookie.mutation = Some(mutationBroker.register(cookie.port))
          } else {
            cookie.mutation.foreach(mutationBroker.deregister _)
            cookie.mutation = None
          }
        }

        !enable
      }
    }
  }
}
