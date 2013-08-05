package com.socrata.datacoordinator.util.collection

import scala.annotation.unchecked.uncheckedVariance
import com.socrata.datacoordinator.id.UserColumnId

class UserColumnIdMapIterator[+V](val underlying: java.util.Iterator[java.util.Map.Entry[String, V @uncheckedVariance]]) extends Iterator[(UserColumnId, V)] {
  private[this] var ent: java.util.Map.Entry[String, V] = _

  def hasNext = underlying.hasNext

  def next() = {
    advance()
    (new UserColumnId(ent.getKey), ent.getValue)
  }
  def advance() {
    ent = underlying.next()
  }

  def key = new UserColumnId(ent.getKey)
  def value = ent.getValue
}
