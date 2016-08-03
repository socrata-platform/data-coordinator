package com.socrata.querycoordinator

import java.util.concurrent.atomic.AtomicInteger

import com.socrata.thirdparty.metrics.Metrics
import com.typesafe.scalalogging.slf4j.Logging

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.compat.Platform
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import spray.caching.LruCache
/**
 * Selects a Secondary Instance.
 *
 * For each dataset, all known instances are divided into two buckets.  The candidates are
 * instances that we either don't know if they have the dataset or that we think have the
 * dataset.  The nots are instances that we think don't have the dataset.
 *
 * For each query, we first pull all the nots from the head of the queue which have expired
 * and move them into the candidates.
 *
 * We then pick an instance from the candidates.  If it is an unexpired There,
 * we return it.  If it is an Unknown or expired There, we check if it has the dataset then
 * update our state accordingly.  Finally, we either return the candidate if we found
 * the dataset, or recurse if we didn't.
 *
 * In the special case that we have no candidate servers, we put all candidates back into the
 * Unknown state to force discovery again.  This is to ensure quick discovery for newly created
 * datasets and allow faster recovery in some types of rolling reboot scenarios.  In order
 * to avoid constant requerying for datasets that are not found for some
 * reason, we only do this if we have hit this case less than x times for this dataset.
 *
 */
class SecondaryInstanceSelector(config: SecondarySelectorConfig) extends Logging with Metrics {
  /** dataset id to servers */
  // TODO: Do we want to keep cache hit ratio?
  private val datasetMap = LruCache[DatasetServers](config.maxCacheEntries)
  /** a count of the number of times we haven't been able to find a server for a dataset. */
  private val datasetNopesMap = LruCache[AtomicInteger](config.maxCacheEntries)

  // The LRU calls use Future.  But we are really doing synchrnized calls in the same process.
  private val waitTime = Duration.Inf

  private val allServers = config.allSecondaryInstanceNames.map { s => Server(s)(Unknown) }.toVector
  private val expirationMillis = config.secondaryDiscoveryExpirationMillis
  private val datasetMaxNopeCount = config.datasetMaxNopeCount

  // ****  Metrics metrics metrics ****
  private val datasetMapSize = metrics.gauge("datasetMap-size") { datasetMap.size }
  private val datasetNopesMapSize = metrics.gauge("datasetNopesMap-size") { datasetNopesMap.size }

  // equality is only on name
  // the code assumes a Server is immutable
  private case class Server(name: String)(val state: ServerState) {
    val verifiedAt: Long = Platform.currentTime

    override def toString: String = s"Server($name => $state)"
  }

  private sealed abstract class ServerState

  private case object There extends ServerState

  private case object NotThere extends ServerState

  private case object Unknown extends ServerState

  private class DatasetServers {
    // optimize for reading a random element from candidates, which is the "normal path".
    /** we either think it is on the secondary, or we don't know */
    var candidates: Vector[Server] = allServers

    // optimize for checking if we need to expire any entries from nots each time we make a query for a ds
    /** we think it is not on the secondary */
    var nots: Queue[Server] = Queue()
  }

  private def pastExpiration(s: Server) = Platform.currentTime > s.verifiedAt + expirationMillis

  private def getNopesCount(dataset: String) = {
    Await.result(datasetNopesMap(dataset)(new AtomicInteger()), waitTime)
  }

  /**
   * Can be called to report an error querying a given dataset on a given server.
   */
  def flagError(dataset: String, secondaryName: String): Unit = {
    // For now we are just going to do the dumb thing and mark it unknown so we
    // at least do a basic validation before we trying to use it again.
    val dss = Await.result(datasetMap(dataset)(new DatasetServers), waitTime)

    val s = Server(secondaryName)(Unknown)

    logger.warn("Notified of error for dataset {} on secondary {}, marking as unknown", dataset, secondaryName)

    dss.synchronized {
      dss.candidates = dss.candidates.filterNot(_ == s)
      dss.nots = dss.nots.filterNot(_ == s)
      dss.candidates = dss.candidates :+ s
    }
  }

  def getInstanceName(dataset: String, isInSecondary: (String => Option[Boolean])): Option[String] = {
    // datasetMap is a synchronized map, then  lock on dss for any access inside dss
    val dss = Await.result(datasetMap(dataset)(new DatasetServers), waitTime)
    @tailrec
    def getInstanceName0: Option[String] = {
      val candidates = candidatesVector(dataset, dss)
      if (candidates.isEmpty) {
        // if it wraps, reset.  it isn't going to wrap.
        if (getNopesCount(dataset).incrementAndGet() < 0) getNopesCount(dataset).set(0)
        logger.debug("Could not find candidate server for dataset {}", dataset)
        None
      } else {
        getNopesCount(dataset).lazySet(0)
        val candidate = candidates(Random.nextInt(candidates.length))
        logger.trace("Candidate {} for dataset {}", candidate, dataset)

        val newCandidateState: ServerState = manufactureNewCandidateState(dataset, isInSecondary, candidate)
        if (newCandidateState != candidate.state) {
          logger.debug("State transition for dataset {} on secondary {} from {} to {}",
            dataset, candidate.name, candidate.state, newCandidateState)
          val newServer = Server(candidate.name)(newCandidateState)

          dss.synchronized {
            // the world may have changed since we got our candidate, so make no assumptions about
            // what is in "candidates" and "nots" at this point, just ensure the final state is what
            // we want.  Keep in mind Server equality is based on name only, so we could replace
            // uses of "candidate" below with "newServer" and it would be functionally equivalent
            dss.candidates = dss.candidates.filterNot(_ == candidate)
            dss.nots = dss.nots.filterNot(_ == candidate)

            newCandidateState match {
              case There | Unknown => dss.candidates = dss.candidates :+ newServer
              case NotThere => dss.nots = dss.nots :+ newServer
            }
          }
        }

        newCandidateState match {
          case There => Some(candidate.name)
          case Unknown | NotThere => getInstanceName0
        }
      }
    }

    syncDSS(dataset, dss)
    val instanceName = getInstanceName0
    logger.info("Accessing dataset {} in {}", dataset, instanceName)
    instanceName
  }

  // Convert any expired nots to unknowns, and set immutable candidates Vector
  private def candidatesVector(dataset: String, dss: DatasetServers): Vector[Server] = {
    dss.synchronized[Vector[Server]] {
      val (expiredNots, remainingNots) = dss.nots.span(s => pastExpiration(s))

      dss.nots = remainingNots
      dss.candidates = dss.candidates ++ expiredNots.map {
        s =>
          logger.debug("State transition for dataset {} on secondary {} from {} to {} on expiration",
            dataset, s.name, s.state, Unknown)
          Server(s.name)(Unknown)
      }

      logger.trace("Current candidates for dataset {}: {}", dataset, dss.candidates)
      logger.trace("Current non-candidates for dataset {}: {}", dataset, dss.nots)

      dss.candidates
    }
  }

  private def manufactureNewCandidateState(dataset: String,
                                           isInSecondary: (String) => Option[Boolean],
                                           candidate: Server): ServerState = {
    candidate.state match {
      case There if pastExpiration(candidate) => Unknown
      case There => There
      case Unknown =>
        try {
          logger.trace("Asking candidate {} if it has dataset {}", candidate, dataset)
          isInSecondary(candidate.name) match {
            case Some(true) => There
            case Some(false) => NotThere
            // might want to think about smarter logic for this case at some point
            case None => NotThere
          }
        } catch {
          case ex: RuntimeException =>
            logger.warn(s"Exception trying to check if candidate $candidate has dataset $dataset", ex)
            NotThere
        }
    }
  }

  private def syncDSS(dataset: String, dss: DatasetServers): Unit = {
    dss.synchronized {
      if (dss.candidates.isEmpty && getNopesCount(dataset).get < datasetMaxNopeCount) {
        logger.debug("Got request for dataset {} with no candidates and low nope count, resetting nots and candidates",
          dataset)
        dss.candidates = allServers
        dss.nots = Queue()
      }
    }
  }
}
