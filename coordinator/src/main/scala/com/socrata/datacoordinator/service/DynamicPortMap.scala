package com.socrata.datacoordinator.service

import com.typesafe.scalalogging.slf4j.Logging

trait DynamicPortMap extends Logging  {

  private val intRx = "(\\d+)".r

  def hostPort(port: Int): Int = {
    Option(System.getenv (s"PORT_$port")) match {
      case Some (intRx (hostPort) ) =>
        logger.info ("host_port: {} -> container_port: {}", hostPort, port.toString)
        hostPort.toInt
      case _ => port
    }
  }
}
