package com.socrata.datacoordinator.packets

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

class PacketsSink extends Packets {
  private val buf = new ListBuffer[Packet]

  def results = buf.toList

  def send(packet: Packet, timeout: Duration) {
    buf += packet
  }

  def receive(timeout: Duration): Option[Packet] =
    sys.error("Cannot call receive on a PacketsSink")

  def poll(): Option[Packet] =
    sys.error("Cannot call poll on a PacketsSink")

  def close() {}
}
