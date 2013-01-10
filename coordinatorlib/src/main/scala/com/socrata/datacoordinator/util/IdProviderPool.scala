package com.socrata.datacoordinator.util

import java.io.Closeable

import com.rojoma.simplearm.{SimpleArm, Managed}

import com.socrata.id.numeric.{PushbackIdProvider, BlockIdProvider, Unallocatable, IdProvider}

trait IdProviderPool extends Closeable {
  type Provider <: IdProvider with Unallocatable

  def borrow(): Managed[Provider]
  def close()
}

class IdProviderPoolImpl(source: BlockIdProvider, strategy: BlockIdProvider => IdProvider) extends IdProviderPool { self =>
  type Provider = PushbackIdProvider

  // always return the one with the most pushed-back items
  private val queue = new java.util.PriorityQueue[PushbackIdProvider](16, Ordering.by { provider => -provider.pendingCount } )

  def borrow() = new SimpleArm[Provider] {
    def flatMap[B](f: (Provider) => B): B = {
      val provider = self.synchronized {
        val existing = queue.poll()
        if(existing == null) new PushbackIdProvider(strategy(source))
        else existing
      }

      try {
        f(provider)
      } finally {
        self.synchronized {
          queue.add(provider)
        }
      }
    }
  }

  def close() {
    synchronized {
      while(!queue.isEmpty) queue.poll().release()
    }
  }
}
