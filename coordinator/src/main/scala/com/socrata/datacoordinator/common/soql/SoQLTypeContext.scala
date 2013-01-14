package com.socrata.datacoordinator.common.soql

import scala.collection.JavaConverters._

import com.socrata.soql.types.{SoQLText, SoQLType}
import com.socrata.datacoordinator.truth.{SimpleRowUserIdMap, RowUserIdMap, TypeContext}
import com.socrata.datacoordinator.id.RowId

object SoQLTypeContext extends TypeContext[SoQLType, Any] {
  def isNull(value: Any): Boolean = SoQLNullValue == value

  def makeValueFromSystemId(id: RowId): Any = id

  def makeSystemIdFromValue(id: Any): RowId = id.asInstanceOf[RowId]

  def nullValue: Any = SoQLNullValue

  private val typesByStringName = SoQLType.typesByName.values.foldLeft(Map.empty[String, SoQLType]) { (acc, typ) =>
    acc + (typ.toString -> typ)
  }
  def typeFromName(name: String): SoQLType = typesByStringName(name)

  def nameFromType(typ: SoQLType): String = typ.toString

  def makeIdMap[T](idColumnType: SoQLType): RowUserIdMap[Any, T] =
    if(idColumnType == SoQLText) {
      new RowUserIdMap[Any, T] {
        val map = new java.util.HashMap[String, (String, T)]

        def put(x: Any, v: T) {
          val s = x.asInstanceOf[String]
          map.put(s.toLowerCase, (s, v))
        }

        def apply(x: Any): T = {
          val s = x.asInstanceOf[String]
          val k = s.toLowerCase
          if(map.containsKey(k)) map.get(k)._2
          else throw new NoSuchElementException
        }

        def get(x: Any): Option[T] = {
          val s = x.asInstanceOf[String]
          val k = s.toLowerCase
          if(map.containsKey(k)) Some(map.get(k)._2)
          else None
        }

        def clear() {
          map.clear()
        }

        def contains(x: Any): Boolean = {
          val s = x.asInstanceOf[String]
          map.containsKey(s.toLowerCase)
        }

        def isEmpty: Boolean = map.isEmpty

        def size: Int = map.size

        def foreach(f: (Any, T) => Unit) {
          val it = map.values.iterator
          while(it.hasNext) {
            val (k, v) = it.next()
            f(k, v)
          }
        }

        def valuesIterator: Iterator[T] =
          map.values.iterator.asScala.map(_._2)
      }
    } else {
      new SimpleRowUserIdMap[Any, T]
    }
}
