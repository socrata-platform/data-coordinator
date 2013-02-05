package com.socrata.datacoordinator.packets
package network

import java.nio.channels.{SelectionKey, SocketChannel}
import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.concurrent.duration.{FiniteDuration, Deadline, Duration}
import scala.concurrent.duration.Duration.Infinite
import java.io.IOException

class NetworkPackets(socket: SocketChannel, val maxPacketSize: Int) extends Packets {
  import NetworkPackets._

  socket.configureBlocking(false)

  private val receiveBuffer = ByteBuffer.allocate(8096)
  private var receiveBufferInWriteMode = true
  private val packetAccumulator = new PacketAccumulator(maxPacketSize)

  private val selector = socket.provider.openSelector()
  private val key = socket.register(selector, 0)

  def close() {
    selector.close()
  }

  private def bufferToReadMode() {
    if(receiveBufferInWriteMode) { receiveBuffer.flip(); receiveBufferInWriteMode = false }
  }

  private def bufferToWriteMode() {
    if(!receiveBufferInWriteMode) { receiveBuffer.compact(); receiveBufferInWriteMode = true }
  }

  private def readAvailablePacket(): ReadResult = {
    bufferToReadMode()
    if(receiveBuffer.hasRemaining) {
      val p = packetAccumulator.accumulate(receiveBuffer)
      if(p.isDefined) return PacketAvailable(p.get)
    }

    @tailrec
    def loop(): ReadResult = {
      bufferToWriteMode()
      socket.read(receiveBuffer) match {
        case 0 =>
          NoPacket
        case -1 =>
          bufferToReadMode()
          if(receiveBuffer.hasRemaining || packetAccumulator.partial) throw new PartialPacket
          EOF
        case n =>
          bufferToReadMode()
          val p = packetAccumulator.accumulate(receiveBuffer)
          if(p.isDefined) return PacketAvailable(p.get)
          loop()
      }
    }
    loop()
  }

  private def deadline(timeout: Duration): Deadline =
    timeout match {
      case finite: FiniteDuration =>
        finite.fromNow
      case _: Infinite =>
        null
    }

  def send(packet: Packet, timeout: Duration) =
    try {
      sendPacketBefore(packet, deadline(timeout))
    } catch {
      case e: IOException =>
        throw new IOProblem(e)
    }

  private def sendPacketBefore(packet: Packet, deadline: Deadline) {
    val buffer = packet.buffer
    do {
      while(buffer.hasRemaining && socket.write(buffer) != 0) {}
      if(buffer.hasRemaining) {
        await(SelectionKey.OP_READ | SelectionKey.OP_WRITE, deadline)

        // FIXME: This will report the wrong error if it's caused by
        // the other end closing its socket.
        if(key.isReadable && !key.isWritable) throw new UnexpectedPacket
      }
    } while(buffer.hasRemaining)
  }

  def receive(timeout: Duration): Option[Packet] =
    try {
      receivePacketBefore(deadline(timeout))
    } catch {
      case e: IOException =>
        throw new IOProblem(e)
    }

  @tailrec
  private def receivePacketBefore(deadline: Deadline): Option[Packet] = {
    readAvailablePacket() match {
      case PacketAvailable(packet) =>
        Some(packet)
      case EOF =>
        None
      case NoPacket =>
        await(SelectionKey.OP_READ, deadline)
        receivePacketBefore(deadline)
    }
  }

  private def await(ops: Int, deadline: Deadline) {
    key.interestOps(ops)
    println(Thread.currentThread.getName + " blocking")
    if(deadline == null) selector.select()
    else {
      val pause = deadline.timeLeft.toMillis
      val count =
        if(pause <= 0) selector.selectNow()
        else selector.select(pause)
      if(count == 0) throw new TimeoutException()
    }
  }
}

object NetworkPackets {
  private sealed abstract class ReadResult
  private case object NoPacket extends ReadResult
  private case object EOF extends ReadResult
  private case class PacketAvailable(packet: Packet) extends ReadResult
}
