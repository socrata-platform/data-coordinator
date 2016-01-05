package com.socrata.querycoordinator.caching

import java.io.{InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.rojoma.simplearm.v2.ResourceScope

/**
 * Allows wrapping the streams used to access the temporary files, in order to (for
 * example) compress them.
 */
trait StreamWrapper {
  /**
   * @return either `in` or a new stream with a transitive close dependency on `in`
   */
  def wrapInputStream(in: InputStream, rs: ResourceScope): InputStream

  /**
   * @return either `out` or a new stream with a transitive close dependency on `out`
   */
  def wrapOutputStream(out: OutputStream, rs: ResourceScope): OutputStream
}

object StreamWrapper {
  /** The default stream wrapper, which simply returns the given streams unchanged. */
  val noop= new StreamWrapper {
    override def wrapInputStream(in: InputStream, rs: ResourceScope): InputStream = in
    override def wrapOutputStream(out: OutputStream, rs: ResourceScope): OutputStream = out
  }

  /** A stream wrapper which applies GZIP compression to temporary data.  You probably
    * don't want to use this, but it's here for completeness. */
  val gzip = new StreamWrapper {
    override def wrapInputStream(in: InputStream, rs: ResourceScope): InputStream =
      rs.open(new GZIPInputStream(in), transitiveClose = List(in))
    override def wrapOutputStream(out: OutputStream, rs: ResourceScope): OutputStream =
      rs.open(new GZIPOutputStream(out), transitiveClose = List(out))
  }

  /** A stream wrapper which applies Snappy compression to temporary data. */
  /*
  val snappy = new StreamWrapper {
    override def wrapInputStream(in: InputStream, rs: ResourceScope): InputStream =
      rs.open(new SnappyInputStream(in), transitiveClose = List(in))
    override def wrapOutputStream(out: OutputStream, rs: ResourceScope): OutputStream =
      rs.open(new SnappyOutputStream(out), transitiveClose = List(out))
  }
  */
}
