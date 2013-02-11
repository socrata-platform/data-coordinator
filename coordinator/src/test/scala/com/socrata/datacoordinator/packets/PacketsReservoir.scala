package com.socrata.datacoordinator.packets

import scala.concurrent.duration.Duration
import scala.collection.mutable

class PacketsReservoir(packets: Packet*) extends Packets {
  val q = mutable.Queue(packets : _*)

  def send(packet: Packet, timeout: Duration) {
    sys.error("Cannot call send on a PacketsReservoir")
  }

  def receive(timeout: Duration): Option[Packet] =
    if(q.isEmpty) None
    else Some(q.dequeue())

  def poll(): Option[Packet] = if(q.isEmpty) None else Some(q.dequeue())

  def close() {}
}
