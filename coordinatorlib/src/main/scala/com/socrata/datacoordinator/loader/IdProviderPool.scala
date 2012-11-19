package com.socrata.datacoordinator.loader

import java.io.Closeable

import com.socrata.id.numeric.{PushbackIdProvider, BlockIdProvider, Unallocatable, IdProvider}

trait IdProviderPool extends Closeable {
  type Provider <: IdProvider with Unallocatable

  def withProvider[T](f: Provider => T): T = {
    val p = borrow()
    try {
      f(p)
    } finally {
      release(p)
    }
  }

  def borrow(): Provider
  def release(borrowed: Provider)
  def close()
}

class IdProviderPoolImpl(source: BlockIdProvider, strategy: BlockIdProvider => IdProvider) extends IdProviderPool {
  type Provider = PushbackIdProvider

  // always return the one with the most pushed-back items
  private val queue = new java.util.PriorityQueue[PushbackIdProvider](16, Ordering.by { provider => -provider.pendingCount } )

  def borrow() = synchronized {
    val existing = queue.poll()
    if(existing == null) new PushbackIdProvider(strategy(source))
    else existing
  }

  def release(borrowed: PushbackIdProvider) = synchronized {
    queue.add(borrowed)
  }

  def close() {
    synchronized {
      while(!queue.isEmpty) queue.poll().release()
    }
  }
}
