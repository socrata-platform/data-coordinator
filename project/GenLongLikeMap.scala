import java.io.{FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets.UTF_8

import sbt._

object GenLongLikeMap {
  def apply(targetDir: File, targetPackage: String, sourcePackage: String, sourceClass: String): Seq[File] = {
    val targetPackageDir = targetPackage.split("/").foldLeft(targetDir)(_ / _)
    Seq(
      writeFile(targetPackageDir, sourceClass + "Map.scala", genImmutableMap(targetPackage, sourcePackage, sourceClass)),
      writeFile(targetPackageDir, sourceClass + "MapIterator.scala", genMapIterator(targetPackage, sourcePackage, sourceClass)),
      writeFile(targetPackageDir, "Mutable" + sourceClass + "Map.scala", genMutableMap(targetPackage, sourcePackage, sourceClass)),
      writeFile(targetPackageDir, sourceClass + "Set.scala", genSet(targetPackage, sourcePackage, sourceClass)),
      writeFile(targetPackageDir, sourceClass + "MutableSet.scala", genMutableSet(targetPackage, sourcePackage, sourceClass))
    )
  }

  def lastElemOf(pkgName: String) = pkgName.split('.').last

  def writeFile(dir: File, file: String, data: String): File = {
    val target = dir / file
    dir.mkdirs()
    val w = new OutputStreamWriter(new FileOutputStream(target), UTF_8)
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

    val sb = new StringBuilder

    sb.append("""
package """).append(targetPackage).append("""

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._

import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.iterator.TLongObjectIterator

object """).append(targetClassName).append(""" {
""")

    sb.append("""
  def apply[V](kvs: (""").append(sourceType).append(""", V)*): """).append(targetClassName).append("""[V] = {
    val tmp = new """).append(mutableVersion).append("""[V]
    tmp ++= kvs
    tmp.freeze()
  }
""")

    sb.append("""
  def apply[V](orig: Map[""").append(sourceType).append(""", V]): """).append(targetClassName).append("""[V] =
    Mutable""").append(sourceClass).append("""Map(orig).freeze()
""")

    sb.append("""
  private val EMPTY = """).append(targetClassName).append("""[Nothing]()

  def empty[V]: """).append(targetClassName).append("""[V] = EMPTY
""")

    sb.append("""
}
""")

    sb.append("""
class """).append(targetClassName).append("""[+V] private[""").append(lastElemOf(targetPackage)).append("""] (val unsafeUnderlying: TLongObjectHashMap[V @uncheckedVariance]) {
""")

    sb.append("""
  @inline def size = unsafeUnderlying.size

  @inline def isEmpty = unsafeUnderlying.isEmpty

  @inline def nonEmpty = !isEmpty

  @inline def contains(t: """).append(sourceType).append(""") = unsafeUnderlying.contains(t.underlying)
""")

    sb.append("""
  @inline def get(t: """).append(sourceType).append(""") = {
    val x = unsafeUnderlying.get(t.underlying)
    if (x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }
""")

    sb.append("""
  @inline def apply(t: """).append(sourceType).append(""") = {
    val x = unsafeUnderlying.get(t.underlying)
    if (x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }
""")

    sb.append("""
  def iterator = new """).append(targetClassName).append("""Iterator[V](unsafeUnderlying.iterator)
""")

    sb.append("""
  def ++[V2 >: V](that: """).append(targetClassName).append("""[V2]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    tmp.putAll(that.unsafeUnderlying)
    new """).append(targetClassName).append("""[V2](tmp)
  }
""")

    sb.append("""
  def ++[V2 >: V](that: """).append(mutableVersion).append("""[V2]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    tmp.putAll(that.underlying)
    new """).append(targetClassName).append("""[V2](tmp)
  }
""")

    sb.append("""
  def ++[V2 >: V](that: Iterable[(""").append(sourceType).append(""", V2)]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    for((k, v) <- that) {
      tmp.put(k.underlying, v)
    }
    new """).append(targetClassName).append("""[V2](tmp)
  }
""")

    sb.append("""
  def +[V2 >: V](kv: (""").append(sourceType).append(""", V2)) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    tmp.put(kv._1.underlying, kv._2)
    new """).append(targetClassName).append("""[V2](tmp)
  }
""")

    sb.append("""
  def -(k: """).append(sourceType).append(""") = {
    val tmp = new TLongObjectHashMap[V](this.unsafeUnderlying)
    tmp.remove(k.underlying)
    new """).append(targetClassName).append("""[V](tmp)
  }
""")

    sb.append("""
  @inline def getOrElse[B >: V](k: """).append(sourceType).append(""", v: => B): B = {
    val result = unsafeUnderlying.get(k.underlying)
    if (result == null) v
    else result
  }
""")

    sb.append("""
  @inline def getOrElseStrict[B >: V](k: """).append(sourceType).append(""", v: B): B = {
    val result = unsafeUnderlying.get(k.underlying)
    if (result == null) v
    else result
  }
""")

    sb.append("""
  def keys: Iterator[""").append(sourceType).append("""] = iterator.map(_._1)
  def values: Iterable[V] = unsafeUnderlying.valueCollection.asScala
  def keySet = new """).append(sourceClass).append("""Set(unsafeUnderlying.keySet)
""")

    sb.append("""
  def mapValuesStrict[V2](f: V => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.value))
    }
    new """).append(targetClassName).append("""[V2](x)
  }
""")

    sb.append("""
  def transform[V2](f: (""").append(sourceType).append(""", V) => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(new """).append(sourceType).append("""(it.key), it.value))
    }
    new """).append(targetClassName).append("""[V2](x)
  }
""")

    sb.append("""
  def foldLeft[S](init: S)(f: (S, (""").append(sourceType).append(""", V)) => S): S =  {
    var seed = init
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      seed = f(seed, (new """).append(sourceType).append("""(it.key), it.value))
    }
    seed
  }
""")

    sb.append("""
  override def toString = unsafeUnderlying.toString
""")

    sb.append("""
  def toSeq: Seq[(""").append(sourceType).append(""", V)] = {
    val arr = new Array[(""").append(sourceType).append(""", V)](unsafeUnderlying.size)
    val it = unsafeUnderlying.iterator
    var i = 0
    while(it.hasNext) {
      it.advance()
      arr(i) = (new """).append(sourceType).append("""(it.key), it.value)
      i += 1
    }
    arr
  }
""")

    sb.append("""
  def foreach[U](f: ((""").append(sourceType).append(""", V)) => U) {
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      f((new """).append(sourceType).append("""(it.key), it.value))
    }
  }
""")

    sb.append("""
  def foreach[U](f: (""").append(sourceType).append(""", V) => U) {
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      f(new """).append(sourceType).append("""(it.key), it.value)
    }
  }
""")

    sb.append("""
  def filter(f: (""").append(sourceType).append(""", V) => Boolean) = {
    val x = new TLongObjectHashMap[V]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      if (f(new """).append(sourceType).append("""(it.key), it.value)) x.put(it.key, it.value)
    }
    new """).append(targetClassName).append("""[V](x)
  }
""")

    sb.append("""
  def filterNot(f: (""").append(sourceType).append(""", V) => Boolean) = {
    val x = new TLongObjectHashMap[V]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      if (!f(new """).append(sourceType).append("""(it.key), it.value)) x.put(it.key, it.value)
    }
    new """).append(targetClassName).append("""[V](x)
  }
""")

    sb.append("""
  override def hashCode = unsafeUnderlying.hashCode
  override def equals(o: Any) = o match {
    case that: """).append(targetClassName).append("""[_] => this.unsafeUnderlying == that.unsafeUnderlying
    case _ => false
  }
""")

    sb.append("""
}
""")

    sb.toString()
  }

  def genMapIterator(targetPackage: String, sourcePackage: String, sourceClass: String): String = {
    val targetClassName = sourceClass + "MapIterator"
    val sourceType = sourcePackage + "." + sourceClass

    val sb = new StringBuilder

    sb.append("""
package """).append(targetPackage).append("""

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._

import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.iterator.TLongObjectIterator

class """).append(targetClassName).append("""[+V](val underlying: TLongObjectIterator[V @uncheckedVariance]) extends Iterator[(""").append(sourceType).append(""", V)] {
""")

    sb.append("""
  def hasNext = underlying.hasNext
  def next() = {
    advance()
    (new """).append(sourceType).append("""(underlying.key), underlying.value)
  }
  def advance() {
    underlying.advance()
  }
  def key = new """).append(sourceType).append("""(underlying.key)
  def value = underlying.value
""")

    sb.append("""
}
""")

    sb.toString()
  }

  def genMutableMap(targetPackage: String, sourcePackage: String, sourceClass: String): String = {
    val immutableVersion = sourceClass + "Map"
    val targetClassName = "Mutable" + sourceClass + "Map"
    val sourceType = sourcePackage + "." + sourceClass

    val sb = new StringBuilder

    sb.append("""
package """).append(targetPackage).append("""

import scala.collection.JavaConverters._
import gnu.trove.map.hash.TLongObjectHashMap

object """).append(targetClassName).append(""" {
""")

    sb.append("""
  private def copyToTMap[V](m: Map[""").append(sourceType).append(""", V]) = {
    val result = new TLongObjectHashMap[V]
    for((k, v) <- m) {
      if (v.asInstanceOf[AnyRef] eq null) throw new NullPointerException("Cannot store null values here")
      result.put(k.underlying, v)
    }
    result
  }
""")

    sb.append("""
  def apply[V](kvs: (""").append(sourceType).append(""", V)*): """).append(targetClassName).append("""[V] = {
    val result = new """).append(targetClassName).append("""[V]
    result ++= kvs
    result
  }
""")

    sb.append("""
  def apply[V](kvs: """).append(immutableVersion).append("""[V]): """).append(targetClassName).append("""[V] = {
    val result = new """).append(targetClassName).append("""[V]
    result ++= kvs
    result
  }
""")

    sb.append("""
  def apply[V](orig: Map[""").append(sourceType).append(""", V]): """).append(targetClassName).append("""[V] =
    new """).append(targetClassName).append("""(copyToTMap(orig))
""")

    sb.append("""
}

class """).append(targetClassName).append("""[V](private var _underlying: TLongObjectHashMap[V]) {
""")

    sb.append("""
  def this() = this(new TLongObjectHashMap[V])
  def this(orig: """).append(targetClassName).append("""[V]) = this(new TLongObjectHashMap(orig.underlying))
  def this(orig: """).append(immutableVersion).append("""[V]) = this(new TLongObjectHashMap(orig.unsafeUnderlying))
""")

    sb.append("""
  def underlying = _underlying
""")

    sb.append("""
  def freeze() = {
    if (underlying == null) throw new NullPointerException
    val result = new """).append(immutableVersion).append("""[V](underlying)
    _underlying = null
    result
  }
""")

    sb.append("""
  def frozenCopy() = {
    if (underlying == null) throw new NullPointerException
    val tmp = new TLongObjectHashMap[V](_underlying)
    new """).append(immutableVersion).append("""[V](tmp)
  }
""")

    sb.append("""
  @inline def size = underlying.size

  @inline def isEmpty = underlying.isEmpty

  @inline def nonEmpty = !underlying.isEmpty

  @inline def contains(t: """).append(sourceType).append(""") = underlying.contains(t.underlying)
""")

    sb.append("""
  @inline def get(t: """).append(sourceType).append(""") = {
    val x = underlying.get(t.underlying)
    if (x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }
""")

    sb.append("""
  @inline def apply(t: """).append(sourceType).append(""") = {
    val x = underlying.get(t.underlying)
    if (x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }
""")

    sb.append("""
  def iterator = new """).append(sourceClass).append("""MapIterator[V](underlying.iterator)
""")

    sb.append("""
  def ++(that: """).append(targetClassName).append("""[V]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.underlying)
    new """).append(immutableVersion).append("""[V](tmp)
  }
""")

    sb.append("""
  def ++(that: """).append(immutableVersion).append("""[V]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.unsafeUnderlying)
    new """).append(immutableVersion).append("""[V](tmp)
  }
""")

    sb.append("""
  def ++(that: Iterable[(""").append(sourceType).append(""", V)]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    for((k, v) <- that) {
      tmp.put(k.underlying, v)
    }
    new """).append(immutableVersion).append("""[V](tmp)
  }
""")

    sb.append("""
  def ++=(that: """).append(targetClassName).append("""[V]) {
    this.underlying.putAll(that.underlying)
  }
""")

    sb.append("""
  def ++=(that: """).append(immutableVersion).append("""[V]) {
    this.underlying.putAll(that.unsafeUnderlying)
  }
""")

    sb.append("""
  def ++=(that: Iterable[(""").append(sourceType).append(""", V)]) {
    for(kv <- that) {
      this += kv
    }
  }
""")

    sb.append("""
  @inline def +=(kv: (""").append(sourceType).append(""", V)) {
    update(kv._1, kv._2)
  }
""")

    sb.append("""
  @inline def -=(k: """).append(sourceType).append(""") {
    underlying.remove(k.underlying)
  }
""")

    sb.append("""
  @inline def update(k: """).append(sourceType).append(""", v: V) {
    if (v.asInstanceOf[AnyRef] eq null) throw new NullPointerException("Cannot store null values here")
    underlying.put(k.underlying, v)
  }
""")

    sb.append("""
  @inline def getOrElse[B >: V](k: """).append(sourceType).append(""", v: => B): B = {
    val result = underlying.get(k.underlying)
    if (result == null) v
    else result
  }
""")

    sb.append("""
  def keys: Iterator[""").append(sourceType).append("""] = iterator.map(_._1)
  def values: Iterable[V] = underlying.valueCollection.asScala
  def keySet = new """).append(sourceClass).append("""Set(underlying.keySet)
""")

    sb.append("""
  def mapValuesStrict[V2](f: V => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.value))
    }
    new """).append(immutableVersion).append("""[V2](x)
  }
""")

    sb.append("""
  def transform[V2](f: (""").append(sourceType).append(""", V) => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(new """).append(sourceType).append("""(it.key), it.value))
    }
    new """).append(immutableVersion).append("""[V2](x)
  }
""")

    sb.append("""
  def foldLeft[S](init: S)(f: (S, (""").append(sourceType).append(""", V)) => S): S =  {
    var seed = init
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      seed = f(seed, (new """).append(sourceType).append("""(it.key), it.value))
    }
    seed
  }
""")

    sb.append("""
  def clear() {
    underlying.clear()
  }
""")

    sb.append("""
  override def toString = underlying.toString
""")

    sb.append("""
  def toSeq = iterator.toSeq
""")

    sb.append("""
  override def hashCode = underlying.hashCode
  override def equals(o: Any) = o match {
    case that: """).append(targetClassName).append("""[_] => this.underlying == that.underlying
    case _ => false
  }
""")

    sb.append("""
}
""")

    sb.toString()
  }

  def genSet(targetPackage: String, sourcePackage: String, sourceClass: String): String = {
    val targetClassName = sourceClass + "Set"
    val sourceType = sourcePackage + "." + sourceClass

    val sb = new StringBuilder

    sb.append("""
package """).append(targetPackage).append("""

import gnu.trove.set.TLongSet
import gnu.trove.set.hash.TLongHashSet
import gnu.trove.procedure.TLongProcedure

object """).append(targetClassName).append(""" {
""")

    sb.append("""
  def apply(xs: """).append(sourceType).append("""*): """).append(targetClassName).append(""" = {
    val tmp = new TLongHashSet
    xs.foreach { x => tmp.add(x.underlying) }
    new """).append(targetClassName).append("""(tmp)
  }
""")

    sb.append("""
  val empty = apply()
""")

    sb.append("""
}
""")

    sb.append("""
class """).append(targetClassName).append("""(val unsafeUnderlying: TLongSet) extends (""").append(sourceType).append(""" => Boolean) {
""")

    sb.append("""
  def apply(x: """).append(sourceType).append(""") = unsafeUnderlying.contains(x.underlying)
""")

    sb.append("""
  def contains(x: """).append(sourceType).append(""") = unsafeUnderlying.contains(x.underlying)
""")

    sb.append("""
  def iterator: Iterator[""").append(sourceType).append("""] = new Iterator[""").append(sourceType).append("""] {
    val it = unsafeUnderlying.iterator
    def hasNext = it.hasNext
    def next() = new """).append(sourceType).append("""(it.next())
  }
""")

    sb.append("""
  def intersect(that: """).append(targetClassName).append("""): """).append(targetClassName).append(""" = {
    if (this.unsafeUnderlying.size <= that.unsafeUnderlying.size) {
      val filter = that.unsafeUnderlying
      val it = unsafeUnderlying.iterator
      val target = new TLongHashSet
      while(it.hasNext) {
        val elem = it.next()
        if (filter.contains(elem)) target.add(elem)
      }
      new """).append(targetClassName).append("""(target)
    } else {
      that.intersect(this)
    }
  }
""")

    sb.append("""
  def -(x: """).append(sourceType).append(""") = {
    val copy = new TLongHashSet(unsafeUnderlying)
    copy.remove(x.underlying)
    new """).append(targetClassName).append("""(copy)
  }
""")

    sb.append("""
  def --(xs: """).append(targetClassName).append(""") = {
    val copy = new TLongHashSet(unsafeUnderlying)
    copy.removeAll(xs.unsafeUnderlying)
    new """).append(targetClassName).append("""(copy)
  }
""")

    sb.append("""
  def foreach[U](f: """).append(sourceType).append(""" => U) {
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        f(new """).append(sourceType).append("""(l))
        true
      }
    })
  }
""")

    sb.append("""
  def size = unsafeUnderlying.size
""")

    sb.append("""
  def isEmpty = unsafeUnderlying.isEmpty
  def nonEmpty = !unsafeUnderlying.isEmpty
""")

    sb.append("""
  def filter(f: """).append(sourceType).append(""" => Boolean) = {
    val result = new TLongHashSet
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        if (f(new """).append(sourceType).append("""(l))) result.add(l)
        true
      }
    })
    new """).append(targetClassName).append("""(result)
  }
""")

    sb.append("""
  def filterNot(f: """).append(sourceType).append(""" => Boolean) = {
    val result = new TLongHashSet
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        if (!f(new """).append(sourceType).append("""(l))) result.add(l)
        true
      }
    })
    new """).append(targetClassName).append("""(result)
  }
""")

    sb.append("""
  def partition(f: """).append(sourceType).append(""" => Boolean) = {
    val yes = new TLongHashSet
    val no = new TLongHashSet
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        if (f(new """).append(sourceType).append("""(l))) yes.add(l)
        else no.add(l)
        true
      }
    })
    (new """).append(targetClassName).append("""(yes), new """).append(targetClassName).append("""(no))
  }
""")

    sb.append("""
  def toSet = {
    val b = Set.newBuilder[""").append(sourceType).append("""]
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        b += new """).append(sourceType).append("""(l)
        true
      }
    })
    b.result()
  }
""")

    sb.append("""
  override def hashCode = unsafeUnderlying.hashCode
  override def equals(o: Any) = o match {
    case that: """).append(targetClassName).append(""" => this.unsafeUnderlying == that.unsafeUnderlying
    case _ => false
  }
""")

    sb.append("""
  override def toString = unsafeUnderlying.toString
""")

    sb.append("""
}
""")

    sb.toString()
  }

  def genMutableSet(targetPackage: String, sourcePackage: String, sourceClass: String): String = {
    val targetClassName = "Mutable" + sourceClass + "Set"
    val sourceType = sourcePackage + "." + sourceClass

    val sb = new StringBuilder

    sb.append("""
package """).append(targetPackage).append("""

import gnu.trove.set.TLongSet
import gnu.trove.set.hash.TLongHashSet
import gnu.trove.procedure.TLongProcedure

object """).append(targetClassName).append(""" {
""")

    sb.append("""
  def apply(xs: """).append(sourceType).append("""*): """).append(targetClassName).append(""" = {
    val tmp = new TLongHashSet
    xs.foreach { x => tmp.add(x.underlying) }
    new """).append(targetClassName).append("""(tmp)
  }
""")

    sb.append("""
  val empty = apply()
""")

    sb.append("""
}
""")

    sb.append("""
class """).append(targetClassName).append("""(private var _underlying: TLongSet) extends (""").append(sourceType).append(""" => Boolean) {
""")

    sb.append("""
  def underlying = _underlying
""")

    sb.append("""
  def apply(x: """).append(sourceType).append(""") = underlying.contains(x.underlying)
""")

    sb.append("""
  def contains(x: """).append(sourceType).append(""") = underlying.contains(x.underlying)
""")

    sb.append("""
  def iterator: Iterator[""").append(sourceType).append("""] = new Iterator[""").append(sourceType).append("""] {
    val it = underlying.iterator
    def hasNext = it.hasNext
    def next() = new """).append(sourceType).append("""(it.next())
  }
""")

    sb.append("""
  @inline
  def foreach[U](f: """).append(sourceType).append(""" => U): Unit = {
    val it = underlying.iterator
    while(it.hasNext) f(new """).append(sourceType).append("""(it.next()))
  }
""")

    sb.append("""
  def freeze() = {
    if (underlying == null) throw new NullPointerException
    val result = new """).append(sourceClass).append("""Set(underlying)
    _underlying = null
    result
  }
""")

    sb.append("""
  def ++=(x: """).append(sourceClass).append("""Set) {
    underlying.addAll(x.unsafeUnderlying)
  }
""")

    sb.append("""
  def +=(x: """).append(sourceType).append(""") {
    underlying.add(x.underlying)
  }
""")

    sb.append("""
  def -=(x: """).append(sourceType).append(""") {
    underlying.remove(x.underlying)
  }
""")

    sb.append("""
  def size = underlying.size
""")

    sb.append("""
  def isEmpty = underlying.isEmpty
  def nonEmpty = !underlying.isEmpty
""")

    sb.append("""
  def clear() {
    underlying.clear()
  }
""")

    sb.append("""
  def toSet = {
    val b = Set.newBuilder[""").append(sourceType).append("""]
    underlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        b += new """).append(sourceType).append("""(l)
        true
      }
    })
    b.result()
  }
""")

    sb.append("""
  override def hashCode = underlying.hashCode
  override def equals(o: Any) = o match {
    case that: """).append(targetClassName).append(""" => this.underlying == that.underlying
    case _ => false
  }
""")

    sb.append("""
  override def toString = underlying.toString
""")

    sb.append("""
}
""")

    sb.toString()
  }
}
