package com.socrata.datacoordinator.packets

import java.io.IOException

class PacketException(cause: Throwable = null) extends Exception(cause)
class TimeoutException extends PacketException
class IOProblem(val underlying: IOException) extends PacketException(underlying)
class ProtocolError extends PacketException
class BadPacketSize(val size: Int) extends ProtocolError
class UnexpectedPacket extends ProtocolError
class PartialPacket extends ProtocolError
