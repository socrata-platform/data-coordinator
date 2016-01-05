package com.socrata.querycoordinator

import com.socrata.http.client.RequestBuilder
import com.socrata.http.common.AuxiliaryData
import com.socrata.querycoordinator.resources.QueryService
import org.apache.curator.x.discovery.ServiceInstance


import scala.concurrent.duration.FiniteDuration

class Secondary(secondaryProvider: ServiceProviderProvider[AuxiliaryData],
                schemaFetcher: SchemaFetcher,
                secondaryInstance: SecondaryInstanceSelector,
                connectTimeout: FiniteDuration,
                schemaTimeout: FiniteDuration) extends QueryService{

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[Secondary])
  val connectTimeoutMillis = connectTimeout.toMillis.toInt
  val schemaTimeoutMillis = schemaTimeout.toMillis.toInt


  def chosenSecondaryName(forcedSecondaryName: Option[String],
                          dataset: String, copy: Option[String]): Option[String] = {
    forcedSecondaryName.orElse {
      secondaryInstance.getInstanceName(dataset, isInSecondary(_, dataset, copy))
    }
  }


  def serviceInstance(dataset: String, instanceName: Option[String]): ServiceInstance[AuxiliaryData] = {
    val instance = for {
      name <- instanceName
      instance <- Option(secondaryProvider.provider(name).getInstance())
    } yield instance

    instance.getOrElse {
      instanceName.foreach { n => secondaryInstance.flagError(dataset, n) }
      finishRequest(noSecondaryAvailable(dataset))
    }
  }


  def isInSecondary(name: String, dataset: String, copy: Option[String]): Option[Boolean] = {
    // TODO we should either create a separate less expensive method for checking if a dataset
    // is in a secondary, or we should integrate this into schema caching if and when we
    // build that.
    for {
      instance <- Option(secondaryProvider.provider(name).getInstance())
      base <- Some(reqBuilder(instance))
      result <- schemaFetcher(base.receiveTimeoutMS(schemaTimeoutMillis), dataset, copy) match {
        case SchemaFetcher.Successful(newSchema, _, _, _) =>
          Some(true)
        case SchemaFetcher.NoSuchDatasetInSecondary =>
          Some(false)
        case other: SchemaFetcher.Result =>
          log.warn(unexpectedError, other)
          None
      }
    } yield result
  }


  def reqBuilder(secondary: ServiceInstance[AuxiliaryData]): RequestBuilder = {
    val pingTarget = for {
      auxData <- Option(secondary.getPayload)
      pingInfo <- auxData.livenessCheckInfo
    } yield pingInfo

    val rb = RequestBuilder(secondary.getAddress).livenessCheckInfo(pingTarget).connectTimeoutMS(connectTimeoutMillis)

    if(Option(secondary.getSslPort).nonEmpty) {
      rb.secure(true).port(secondary.getSslPort)
    } else if (Option(secondary.getPort).nonEmpty) {
      rb.port(secondary.getPort)
    } else {
      rb
    }
  }

}
object Secondary {
  def apply(secondaryProvider: ServiceProviderProvider[AuxiliaryData],
            schemaFetcher: SchemaFetcher,
            secondaryInstance: SecondaryInstanceSelector,
            connectTimeout: FiniteDuration,
            schemaTimeout: FiniteDuration): Secondary = {

    new Secondary(secondaryProvider, schemaFetcher, secondaryInstance, connectTimeout, schemaTimeout)
  }

}
