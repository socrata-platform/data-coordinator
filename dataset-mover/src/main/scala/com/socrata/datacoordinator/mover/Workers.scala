package com.socrata.datacoordinator.mover

import scala.collection.JavaConverters._
import scala.util.control.Breaks

import java.io.{File, InputStreamReader, BufferedReader}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue

import com.rojoma.simplearm.v2._
import org.slf4j.LoggerFactory

class Workers(parallelism: Int) {
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
      override def run() {
        try {
          runJob(fromInstance, toInstance, systemId)
        } catch {
          case e: Exception =>
            log.error("Unexpected exception running job for {}.{}", fromInstance, systemId.asInstanceOf[AnyRef], e)
            queue.add(CompletedJob(systemId, Instant.now(), false))
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
    val args = Seq.newBuilder[String]
    args += "java"
    for(confFile <- Option(System.getProperty("config.file"))) {
      args += "-Dconfig.file=" + confFile
    }
    args += "-jar"
    args += new File(Main.getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getPath
    args += s"$fromInstance.$systemId"
    args += toInstance

    val proc = new ProcessBuilder(args.result().asJava)
      .inheritIO()
      .redirectOutput(ProcessBuilder.Redirect.PIPE)
      .redirectErrorStream(true)
      .start()

    val successful = try {
      for {
        stdout <- managed(proc.getInputStream)
        isr <- managed(new InputStreamReader(stdout, StandardCharsets.UTF_8))
        br <- managed(new BufferedReader(isr))
      } {
        for(line <- new LinesIterator(br, keepEndOfLine = true)) {
          stdoutMutex.synchronized {
            Console.out.print(s"${fromInstance}.${systemId}: ${line}")
            Console.out.flush()
          }
        }
        log.info("Subprocess stdout closed: {}.{}", fromInstance, systemId)
        proc.waitFor() == 0
      }
    } catch {
      case e: Exception =>
        log.error("Exception while running a job for {}.{}", fromInstance, systemId.asInstanceOf[AnyRef], e)
        queue.add(CompletedJob(systemId, Instant.now(), false))
        proc.destroyForcibly()
        return
    }
    queue.add(CompletedJob(systemId, Instant.now(), successful))
  }
}

object Workers {
  val log = LoggerFactory.getLogger(classOf[Workers])
  case class CompletedJob(systemId: Long, finishedAt: Instant, finishedSuccessFully: Boolean)
}
