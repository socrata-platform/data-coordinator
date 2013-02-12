package com.socrata.datacoordinator.packets

import scala.concurrent.duration.Duration

import java.io.Closeable

/** A sender writes data to a Receiver.  It is assumed to do so quickly;
  * the receiver will consider getting into a state where its transmit buffer
  * has filled up and not drained within its timeout to be an error. */
trait Packets extends Closeable {
  val maxPacketSize: Int

  /** Transmits a message and waits for the response. */
  def send(packet: Packet, timeout: Duration = Duration.Inf)
  def receive(timeout: Duration = Duration.Inf): Option[Packet]

  /** Checks to see if a message is ready.  This is not required to actually check
    * a message source; it is meant to be called after a bulk send to see if anything
    * was received during the send.
    * @note "None" here does NOT necessarily mean EOF. */
  def poll(): Option[Packet]
}
