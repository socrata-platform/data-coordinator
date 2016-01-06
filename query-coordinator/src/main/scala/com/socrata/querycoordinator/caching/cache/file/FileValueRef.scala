package com.socrata.querycoordinator.caching.cache.file

import java.io.InputStream
import java.nio.channels.FileChannel

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.querycoordinator.caching.cache.ValueRef
import com.socrata.querycoordinator.caching.{CloseBlockingInputStream, FileChannelInputStream}
import com.socrata.util.io.StreamWrapper

class FileValueRef(fc: FileChannel, streamWrapper: StreamWrapper, scope: ResourceScope) extends ValueRef {
  private var closed = false

  override def open(rs: ResourceScope): InputStream =
    streamWrapper.wrapInputStream(rs.open(new CloseBlockingInputStream(new FileChannelInputStream(fc))), rs)

  override def close(): Unit = synchronized {
    if(!closed) {
      closed = true
      // there is a slight race condition here, if the session that produced this
      // valueref is being closed concurrently.  I need to add closeIfManaged to
      // ResourceScope to handle that case.
      if(fc.isOpen) scope.close(fc)
    }
  }
}
