package com.socrata.datacoordinator.mover

import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue

import com.rojoma.simplearm.v2._
import org.slf4j.LoggerFactory

import com.socrata.datacoordinator.id.DatasetInternalName

class Workers(parallelism: Int, singleMover: (DatasetInternalName, String) => Unit) {
  import Workers._

  private val stdoutMutex = new Object
  private var running = 0
  private val queue = new ArrayBlockingQueue[CompletedJob](parallelism)

  def submit(fromInstance: String, toInstance: String, systemId: Long): Option[CompletedJob] = {
    val result =
      if(running == parallelism) {
        val completed = queue.take()
        running -= 1
        Some(completed)
      } else {
        None
      }

    val thread = new Thread() {
      setName(s"${fromInstance}.${systemId} -> ${toInstance}")
      override def run() {
        try {
          runJob(fromInstance, toInstance, systemId)
        } catch {
          case e: Exception =>
            log.error("Unexpected exception running job for {}.{}", fromInstance, systemId.asInstanceOf[AnyRef], e)
            queue.add(FailedJob(systemId, Instant.now(), s"Unexpected exception: $e"))
        }
      }
    }
    thread.start()
    running += 1

    result
  }

  def shutdown(): Iterator[Workers.CompletedJob] =
    (0 until running).iterator.map { _ => queue.take() }

  private def runJob(fromInstance: String, toInstance: String, systemId: Long): Unit = {
    try {
      singleMover(DatasetInternalName(s"${fromInstance}.${systemId}").getOrElse(SingleMover.bail("Invalid dataset internal name")), toInstance)
    } catch {
      case SingleMover.Bail(msg) =>
        queue.add(FailedJob(systemId, Instant.now(), msg))
        return
    }
    queue.add(SuccessfulJob(systemId, Instant.now()))
  }
}

object Workers {
  val log = LoggerFactory.getLogger(classOf[Workers])
  sealed abstract class CompletedJob {
    val systemId: Long
    val finishedAt: Instant
  }
  case class SuccessfulJob(systemId: Long, finishedAt: Instant) extends CompletedJob
  case class FailedJob(systemId: Long, finishedAt: Instant, failureReason: String) extends CompletedJob
}
