import java.io.{FileOutputStream, OutputStreamWriter}
import sbt._

object GenLongLikeMap {
  def apply(targetDir: File, targetPackage: String, sourcePackage: String, sourceClass: String): Seq[File] = {
    val targetPackageDir = targetPackage.split("/").foldLeft(targetDir)(_ / _)
    Seq(
      writeFile(targetPackageDir, sourceClass + "Map.scala", genImmutableMap(targetPackage, sourcePackage, sourceClass)),
      writeFile(targetPackageDir, sourceClass + "MapIterator.scala", genMapIterator(targetPackage, sourcePackage, sourceClass)),
      writeFile(targetPackageDir, "Mutable" + sourceClass + "Map.scala", genMutableMap(targetPackage, sourcePackage, sourceClass)),
      writeFile(targetPackageDir, sourceClass + "Set.scala", genSet(targetPackage, sourcePackage, sourceClass))
    )
  }

  def lastElemOf(pkgName: String) = pkgName.split('.').last

  def writeFile(dir: File, file: String, data: String): File = {
    val target = dir / file
    dir.mkdirs()
    val w = new OutputStreamWriter(new FileOutputStream(target), "UTF-8")
    try {
      w.write(data)
    } finally {
      w.close()
    }
    target
  }

  def genImmutableMap(targetPackage: String, sourcePackage: String, sourceClass: String): String = {
    val targetClassName = sourceClass + "Map"
    val mutableVersion = "Mutable" + targetClassName
    val sourceType = sourcePackage + "." + sourceClass
    """
package """ + targetPackage + """

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._

import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.iterator.TLongObjectIterator

object """ + targetClassName + """ {
  def apply[V](kvs: (""" + sourceType + """, V)*): """ + targetClassName + """[V] = {
    val tmp = new """ + mutableVersion + """[V]
    tmp ++= kvs
    tmp.freeze()
  }

  def apply[V](orig: Map[""" + sourceType + """, V]): """ + targetClassName + """[V] =
    Mutable""" + sourceClass + """Map(orig).freeze()

  private val EMPTY = """ + targetClassName + """[Nothing]()

  def empty[V]: """ + targetClassName + """[V] = EMPTY
}

class """ + targetClassName + """[+V] private[""" + lastElemOf(targetPackage) + """] (val unsafeUnderlying: TLongObjectHashMap[V @uncheckedVariance]) {
  @inline def size = unsafeUnderlying.size

  @inline def isEmpty = unsafeUnderlying.isEmpty

  @inline def nonEmpty = !isEmpty

  @inline def contains(t: """ + sourceType + """) = unsafeUnderlying.contains(t.underlying)

  @inline def get(t: """ + sourceType + """) = {
    val x = unsafeUnderlying.get(t.underlying)
    if(x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }

  @inline def apply(t: """ + sourceType + """) = {
    val x = unsafeUnderlying.get(t.underlying)
    if(x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }

  def iterator = new """ + targetClassName + """Iterator[V](unsafeUnderlying.iterator)

  def ++[V2 >: V](that: """ + targetClassName + """[V2]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    tmp.putAll(that.unsafeUnderlying)
    new """ + targetClassName + """[V2](tmp)
  }

  def ++[V2 >: V](that: """ + mutableVersion + """[V2]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    tmp.putAll(that.underlying)
    new """ + targetClassName + """[V2](tmp)
  }

  def ++[V2 >: V](that: Iterable[(""" + sourceType + """, V2)]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    for((k, v) <- that) {
      tmp.put(k.underlying, v)
    }
    new """ + targetClassName + """[V2](tmp)
  }

  @inline def getOrElse[B >: V](k: """ + sourceType + """, v: => B): B = {
    val result = unsafeUnderlying.get(k.underlying)
    if(result == null) v
    else result
  }

  @inline def getOrElseStrict[B >: V](k: """ + sourceType + """, v: B): B = {
    val result = unsafeUnderlying.get(k.underlying)
    if(result == null) v
    else result
  }

  def keys: Iterator[""" + sourceType + """] = iterator.map(_._1)
  def values: Iterable[V] = unsafeUnderlying.valueCollection.asScala

  def keySet = new """ + sourceClass + """Set(unsafeUnderlying.keySet)

  def mapValuesStrict[V2](f: V => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.value))
    }
    new """ + targetClassName + """[V2](x)
  }

  def transform[V2](f: (""" + sourceType + """, V) => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(new """ + sourceType + """(it.key), it.value))
    }
    new """ + targetClassName + """[V2](x)
  }

  def foldLeft[S](init: S)(f: (S, (""" + sourceType + """, V)) => S): S =  {
    var seed = init
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      seed = f(seed, (new """ + sourceType + """(it.key), it.value))
    }
    seed
  }

  override def toString = unsafeUnderlying.toString

  def toSeq: Seq[(""" + sourceType + """, V)] = {
    val arr = new Array[(""" + sourceType + """, V)](unsafeUnderlying.size)
    val it = unsafeUnderlying.iterator
    var i = 0
    while(it.hasNext) {
      it.advance()
      arr(i) = (new """ + sourceType + """(it.key), it.value)
      i += 1
    }
    arr
  }

  def foreach[U](f: ((""" + sourceType + """, V)) => U) {
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      f(new """ + sourceType + """(it.key), it.value)
    }
  }

  def filter(f: (""" + sourceType + """, V) => Boolean) = {
    val x = new TLongObjectHashMap[V]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      if(f(new """ + sourceType + """(it.key), it.value)) x.put(it.key, it.value)
    }
    new """ + targetClassName + """[V](x)
  }

  def filterNot(f: (""" + sourceType + """, V) => Boolean) = {
    val x = new TLongObjectHashMap[V]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      if(!f(new """ + sourceType + """(it.key), it.value)) x.put(it.key, it.value)
    }
    new """ + targetClassName + """[V](x)
  }

  override def hashCode = unsafeUnderlying.hashCode
  override def equals(o: Any) = o match {
    case that: """ + targetClassName + """[_] => this.unsafeUnderlying == that.unsafeUnderlying
    case _ => false
  }
}
"""
  }

  def genMapIterator(targetPackage: String, sourcePackage: String, sourceClass: String): String = {
    val targetClassName = sourceClass + "MapIterator"
    val sourceType = sourcePackage + "." + sourceClass
  """
package """ + targetPackage + """

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._

import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.iterator.TLongObjectIterator

class """ + targetClassName + """[+V](val underlying: TLongObjectIterator[V @uncheckedVariance]) extends Iterator[(""" + sourceType + """, V)] {
def hasNext = underlying.hasNext
  def next() = {
    advance()
    (new """ + sourceType + """(underlying.key), underlying.value)
  }
  def advance() {
    underlying.advance()
  }
  def key = new """ + sourceType + """(underlying.key)
  def value = underlying.value
}
"""
  }

  def genMutableMap(targetPackage: String, sourcePackage: String, sourceClass: String): String = {
    val immutableVersion = sourceClass + "Map"
    val targetClassName = "Mutable" + sourceClass + "Map"
    val sourceType = sourcePackage + "." + sourceClass
    """
package """ + targetPackage + """

import scala.collection.JavaConverters._
import gnu.trove.map.hash.TLongObjectHashMap

object """ + targetClassName + """ {
  private def copyToTMap[V](m: Map[""" + sourceType + """, V]) = {
    val result = new TLongObjectHashMap[V]
    for((k, v) <- m) {
      if(v.asInstanceOf[AnyRef] eq null) throw new NullPointerException("Cannot store null values here")
      result.put(k.underlying, v)
    }
    result
  }

  def apply[V](kvs: (""" + sourceType + """, V)*): """ + targetClassName + """[V] = {
    val result = new """ + targetClassName + """[V]
    result ++= kvs
    result
  }

  def apply[V](kvs: """ + immutableVersion + """[V]): """ + targetClassName + """[V] = {
    val result = new """ + targetClassName + """[V]
    result ++= kvs
    result
  }

  def apply[V](orig: Map[""" + sourceType + """, V]): """ + targetClassName +"""[V] =
    new """ + targetClassName + """(copyToTMap(orig))
}

class """ + targetClassName + """[V](private var _underlying: TLongObjectHashMap[V]) {
  def this() = this(new TLongObjectHashMap[V])
  def this(orig: """ + targetClassName + """[V]) = this(new TLongObjectHashMap(orig.underlying))
  def this(orig: """ + immutableVersion + """[V]) = this(new TLongObjectHashMap(orig.unsafeUnderlying))

  def underlying = _underlying

  def freeze() = {
    if(underlying == null) throw new NullPointerException
    val result = new """ + immutableVersion + """[V](underlying)
    _underlying = null
    result
  }

  @inline def size = underlying.size

  @inline def isEmpty = underlying.isEmpty

  @inline def nonEmpty = !underlying.isEmpty

  @inline def contains(t: """ + sourceType + """) = underlying.contains(t.underlying)

  @inline def get(t: """ + sourceType + """) = {
    val x = underlying.get(t.underlying)
    if(x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }

  @inline def apply(t: """ + sourceType + """) = {
    val x = underlying.get(t.underlying)
    if(x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }

  def iterator = new """ + sourceClass + """MapIterator[V](underlying.iterator)

  def ++(that: """ + targetClassName + """[V]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.underlying)
    new """ + immutableVersion + """[V](tmp)
  }

  def ++(that: """ + immutableVersion + """[V]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.unsafeUnderlying)
    new """ + immutableVersion + """[V](tmp)
  }

  def ++(that: Iterable[(""" + sourceType + """, V)]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    for((k, v) <- that) {
      tmp.put(k.underlying, v)
    }
    new """ + immutableVersion + """[V](tmp)
  }

  def ++=(that: """ + targetClassName + """[V]) {
    this.underlying.putAll(that.underlying)
  }

  def ++=(that: """ + immutableVersion + """[V]) {
    this.underlying.putAll(that.unsafeUnderlying)
  }

  def ++=(that: Iterable[(""" + sourceType + """, V)]) {
    for(kv <- that) {
      this += kv
    }
  }

  @inline def +=(kv: (""" + sourceType + """, V)) {
    update(kv._1, kv._2)
  }

  @inline def -=(k: """ + sourceType + """) {
    underlying.remove(k.underlying)
  }

  @inline def update(k: """ + sourceType + """, v: V) {
    if(v.asInstanceOf[AnyRef] eq null) throw new NullPointerException("Cannot store null values here")
    underlying.put(k.underlying, v)
  }

  @inline def getOrElse[B >: V](k: """ + sourceType + """, v: => B): B = {
    val result = underlying.get(k.underlying)
    if(result == null) v
    else result
  }

  def keys: Iterator[""" + sourceType + """] = iterator.map(_._1)
  def values: Iterable[V] = underlying.valueCollection.asScala

  def keySet = new """ + sourceClass + """Set(underlying.keySet)

  def mapValuesStrict[V2](f: V => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.value))
    }
    new """ + immutableVersion + """[V2](x)
  }

  def transform[V2](f: (""" + sourceType + """, V) => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(new """ + sourceType + """(it.key), it.value))
    }
    new """ + immutableVersion + """[V2](x)
  }

  def foldLeft[S](init: S)(f: (S, (""" + sourceType + """, V)) => S): S =  {
    var seed = init
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      seed = f(seed, (new """ + sourceType + """(it.key), it.value))
    }
    seed
  }

  override def toString = underlying.toString

  def toSeq = iterator.toSeq

  override def hashCode = underlying.hashCode
  override def equals(o: Any) = o match {
    case that: """ + targetClassName + """[_] => this.underlying == that.underlying
    case _ => false
  }
}
"""
  }

  def genSet(targetPackage: String, sourcePackage: String, sourceClass: String): String = {
    val targetClassName = sourceClass + "Set"
    val sourceType = sourcePackage + "." + sourceClass
    """
package """ + targetPackage + """

import gnu.trove.set.TLongSet
import gnu.trove.set.hash.TLongHashSet
import gnu.trove.procedure.TLongProcedure

object """ + targetClassName + """ {
  def apply(xs: """ + sourceType + """*): """ + targetClassName + """ = {
    val tmp = new TLongHashSet
    xs.foreach { x => tmp.add(x.underlying) }
    new """ + targetClassName + """(tmp)
  }

  val empty = apply()
}

class """ + targetClassName + """(val unsafeUnderlying: TLongSet) extends (""" + sourceType + """ => Boolean) {
  def apply(x: """ + sourceType + """) = unsafeUnderlying.contains(x.underlying)

  def contains(x: """ + sourceType + """) = unsafeUnderlying.contains(x.underlying)

  def intersect(that: """ + targetClassName + """): """ + targetClassName + """ = {
    if(this.unsafeUnderlying.size <= that.unsafeUnderlying.size) {
      val filter = that.unsafeUnderlying
      val it = unsafeUnderlying.iterator
      val target = new TLongHashSet
      while(it.hasNext) {
        val elem = it.next()
        if(filter.contains(elem)) target.add(elem)
      }
      new """ + targetClassName + """(target)
    } else {
      that.intersect(this)
    }
  }

  def -(x: """ + sourceType + """) = {
    val copy = new TLongHashSet(unsafeUnderlying)
    copy.remove(x.underlying)
    new """ + targetClassName + """(copy)
  }

  def size = unsafeUnderlying.size

  def isEmpty = unsafeUnderlying.isEmpty
  def nonEmpty = !unsafeUnderlying.isEmpty

  def filter(f: """ + sourceType + """ => Boolean) = {
    val result = new TLongHashSet
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        if(f(new """ + sourceType + """(l))) result.add(l)
        true
      }
    })
    new """ + targetClassName + """(result)
  }

  def filterNot(f: """ + sourceType + """ => Boolean) = {
    val result = new TLongHashSet
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        if(!f(new """ + sourceType + """(l))) result.add(l)
        true
      }
    })
    new """ + targetClassName + """(result)
  }

  def partition(f: """ + sourceType + """ => Boolean) = {
    val yes = new TLongHashSet
    val no = new TLongHashSet
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        if(f(new """ + sourceType + """(l))) yes.add(l)
        else no.add(l)
        true
      }
    })
    (new """ + targetClassName + """(yes), new """ + targetClassName + """(no))
  }

  def toSet = {
    val b = Set.newBuilder[""" + sourceType + """]
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        b += new """ + sourceType + """(l)
        true
      }
    })
    b.result()
  }

  override def hashCode = unsafeUnderlying.hashCode
  override def equals(o: Any) = o match {
    case that: """ + targetClassName + """ => this.unsafeUnderlying == that.unsafeUnderlying
    case _ => false
  }
}
"""
  }
}
