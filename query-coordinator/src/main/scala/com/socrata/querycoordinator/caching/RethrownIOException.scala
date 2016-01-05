package com.socrata.querycoordinator.caching

import java.io.IOException

/** Thrown by `SplitStream` when the slower reader catches up
  * to the point where a faster reader received an IO exception
  * from the underlying stream.
  */
class RethrownIOException(cause: IOException) extends IOException("Exception re-thrown by second reader", cause)
