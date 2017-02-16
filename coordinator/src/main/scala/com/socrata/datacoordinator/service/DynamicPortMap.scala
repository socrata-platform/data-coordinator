package com.socrata.datacoordinator.service

import org.slf4j.Logger

trait DynamicPortMap {

  val log: Logger

  private val intRx = "(\\d+)".r

  def hostPort(port: Int): Int = {
    Option(System.getenv (s"PORT_$port")) match {
      case Some (intRx (hostPort) ) =>
        log.info(s"host_port: $hostPort -> container_port: $port")
        hostPort.toInt
      case _ => port
    }
  }
}
