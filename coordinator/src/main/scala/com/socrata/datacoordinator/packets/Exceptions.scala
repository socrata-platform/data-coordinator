package com.socrata.datacoordinator.packets

import java.io.IOException

class PacketException(msg: String = null, cause: Throwable = null) extends Exception(msg, cause)
class TimeoutException(msg: String = null, cause: Throwable = null) extends PacketException(msg, cause)
class IOProblem(msg: String = null, val underlying: IOException) extends PacketException(msg, underlying)
class ProtocolError(msg: String = null, cause: Throwable = null) extends PacketException(msg, cause)
class BadPacketSize(val size: Int, msg: String = null, cause: Throwable = null) extends ProtocolError(msg, cause)
class UnexpectedPacket(msg: String = null, cause: Throwable = null) extends ProtocolError(msg, cause)
class PartialPacket(msg: String = null, cause: Throwable = null) extends ProtocolError(msg, cause)
