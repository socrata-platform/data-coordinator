package com.socrata.datacoordinator.common.soql

import scala.collection.JavaConverters._
import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.{ComplexTypeRowUserIdMap, RowUserIdMap, SimpleRowUserIdMap, TypeContext}
import com.socrata.datacoordinator.id.{RowId, RowVersion}
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, TypeNamespace}
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.util.collection.MutableRowIdMap

object SoQLTypeContext extends TypeContext[SoQLType, SoQLValue] {
  def isNull(value: SoQLValue): Boolean = SoQLNull == value

  def makeValueFromSystemId(id: RowId): SoQLValue = SoQLID(id.underlying)

  def makeSystemIdFromValue(id: SoQLValue): RowId = new RowId(id.asInstanceOf[SoQLID].value)

  def makeValueFromRowVersion(v: RowVersion): SoQLValue = SoQLVersion(v.underlying)

  def makeRowVersionFromValue(id: SoQLValue): RowVersion = new RowVersion(id.asInstanceOf[SoQLVersion].value)

  def nullValue: SoQLValue = SoQLNull

  val typeNamespace: TypeNamespace[SoQLType] = new TypeNamespace[SoQLType] {
    private val typesByTypeName = SoQLType.typesByName.values.foldLeft(Map.empty[String, SoQLType]) { (acc, typ) =>
      acc + (typ.name.caseFolded -> typ)
    }
    def typeForName(datasetInfo: DatasetInfo, name: String): SoQLType = typesByTypeName(name)

    def nameForType(typ: SoQLType): String = typ.name.caseFolded

    def typeForUserType(name: TypeName): Option[SoQLType] = typesByTypeName.get(name.caseFolded)

    def userTypeForType(typ: SoQLType): TypeName = typ.name
  }

  def makeIdMap[T](idColumnType: SoQLType): RowUserIdMap[SoQLValue, T] =
    if(idColumnType == SoQLID) {
      new RowUserIdMap[SoQLValue, T] {
        val map = new MutableRowIdMap[T]

        def put(x: SoQLValue, v: T): Unit = {
          val id = new RowId(x.asInstanceOf[SoQLID].value)
          map(id) = v
        }

        def remove(x: SoQLValue): Unit = {
          val id = new RowId(x.asInstanceOf[SoQLID].value)
          map -= id
        }

        def apply(x: SoQLValue): T = {
          val id = new RowId(x.asInstanceOf[SoQLID].value)
          map(id)
        }

        def get(x: SoQLValue): Option[T] = {
          val id = new RowId(x.asInstanceOf[SoQLID].value)
          map.get(id)
        }

        def clear(): Unit = {
          map.clear()
        }

        def contains(x: SoQLValue): Boolean = {
          val id = new RowId(x.asInstanceOf[SoQLID].value)
          map.contains(id)
        }

        def isEmpty: Boolean = map.isEmpty

        def size: Int = map.size

        def foreach(f: (SoQLValue, T) => Unit) {
          val it = map.iterator
          while(it.hasNext) {
            it.advance()
            f(new SoQLID(it.key.underlying), it.value)
          }
        }

        def keysIterator: Iterator[SoQLID] = map.keys.map { rid => SoQLID(rid.underlying) }

        def valuesIterator: Iterator[T] =
          map.values.iterator
      }
    } else {
      idColumnType match {
        case SoQLUrl =>
          new ComplexTypeRowUserIdMap[SoQLUrl, SoQLValue, T]
        case _ =>
          new SimpleRowUserIdMap[SoQLValue, T]
      }
    }
}
