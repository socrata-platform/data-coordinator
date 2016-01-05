package com.socrata.querycoordinator.caching

import java.util.concurrent.atomic.AtomicReference

import com.rojoma.simplearm.v2.Resource

import scala.util.control.ControlThrowable

/**
 * A reference to a closeable item which can be shared amongst multiple
 * ResourceScopes or Managed blocks; the underlying thing is closed only
 * when all the `SharedHandle`s referring to it are.
 *
 * It is the user's responsibility to avoid close-races (thus the lack of
 * an "isClosed" method).  As long as these are managed by `ResourceScope`s,
 * that should be straightforward.
 *
 * This should probably become part of simple-arm.
 */
class SharedHandle[T] private (boxRef: AtomicReference[SharedHandle.Box[T]]) {
  private var closed = false

  /** If this handle is still open, creates an independent handle to the
    * referenced object.
    *
    * This method is thread-safe.
    */
  def duplicate(): SharedHandle[T] = {
    def dupClosed() = throw new IllegalStateException("Duplicating a closed handle")
    val box = boxRef.get()
    if(box eq null) dupClosed()
    box.synchronized {
      if(closed || box.refcount == 0) dupClosed()
      val result = new SharedHandle(boxRef)
      box.refcount += 1
      result
    }
  }

  private def doClose(closeAction: SharedHandle.Box[T] => Unit): Unit = {
    val box = boxRef.get()
    if(box ne null) {
      box.synchronized {
        if(!closed) { // prevent issuing several closes through a single handle
          closed = true
          if(box.refcount > 0) {
            box.refcount -= 1
            if(box.refcount == 0) {
              boxRef.set(null)
              closeAction(box)
            }
          }
        }
      }
    }
  }

  def close(): Unit = doClose { box =>
    box.resource.close(box.thing)
  }

  private def closeAbnormally(e: Throwable): Unit = doClose { box =>
    box.resource.closeAbnormally(box.thing, e)
  }

  def get: T = {
    val box = boxRef.get()
    if(box == null || closed) throw new NoSuchElementException("Closed handle")
    // we don't need to double-check the box's refcount here because once you have the thing
    // it's YOUR responsibility to ensure no concurrent closure.  The only reason we check
    // the first time is to prevent a null leaking outside the bounds of this class.
    box.thing
  }
}

object SharedHandle {
  private class Box[T](val thing: T, val resource: Resource[T]) {
    var refcount = 1
  }

  private val erasedHandle = new Resource[SharedHandle[_]] {
    override def close(a: SharedHandle[_]): Unit = a.close()
    override def closeAbnormally(a: SharedHandle[_], e: Throwable): Unit = a.closeAbnormally(e)
  }
  implicit def shResource[T]: Resource[SharedHandle[T]] =
    erasedHandle.asInstanceOf[Resource[SharedHandle[T]]]

  def apply[T](thing: => T)(implicit res: Resource[T]) = {
    val it = thing
    try {
      new SharedHandle[T](new AtomicReference(new Box(it, res)))
    } catch {
      case e: ControlThrowable => // this shouldn't happen
        res.close(it)
        throw e
      case e: Throwable =>
        try {
          res.closeAbnormally(it, e)
        } catch {
          case e2: Throwable =>
            e.addSuppressed(e2)
        }
        throw e
    }
  }
}
