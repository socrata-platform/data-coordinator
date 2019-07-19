package com.socrata.datacoordinator.truth

import com.socrata.soql.types.{SoQLText, SoQLUrl, SoQLValue}

import scala.collection.JavaConverters._

trait SimpleRowId[CV2, CV] {
  def toId(x: CV): CV
}

object SimpleRowId {
  implicit val urlRowId = new SimpleRowId[SoQLUrl, SoQLValue] {
    def toId(x: SoQLValue): SoQLValue = {
      val url = x.asInstanceOf[SoQLUrl]
      url.description match {
        case None => x
        case _ => url.copy(description = None)
      }
    }
  }
}

class ComplexTypeRowUserIdMap[CV2, CV, T](implicit sh: SimpleRowId[CV2, CV]) extends RowUserIdMap[CV, T] {
  val map = new java.util.HashMap[CV, T]

  def put(x: CV, v: T) {
    val sx = sh.toId(x)
    map.put(sx, v)
  }

  def apply(x: CV): T = {
    val sx = sh.toId(x)
    if(!map.containsKey(sx)) throw new NoSuchElementException
    map.get(sx)
  }

  def get(x: CV): Option[T] = {
    val sx = sh.toId(x)
    if (!map.containsKey(sx)) None
    else Some(map.get(sx))
  }

  def remove(x: CV) {
    map.remove(sh.toId(x))
  }

  def clear() { map.clear() }

  def contains(x: CV): Boolean = map.containsKey(sh.toId(x))

  def isEmpty: Boolean = map.isEmpty

  def size: Int = map.size

  def foreach(f: (CV, T) => Unit) {
    val it = map.entrySet().iterator
    while(it.hasNext) {
      val ent = it.next()
      f(ent.getKey, ent.getValue)
    }
  }

  def keysIterator = map.keySet.iterator.asScala

  def valuesIterator: Iterator[T] = map.values.iterator.asScala
}
