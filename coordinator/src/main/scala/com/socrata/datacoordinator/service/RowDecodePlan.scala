package com.socrata.datacoordinator
package service

import com.rojoma.json.v3.ast.{JArray, JBoolean, JObject, JValue}
import com.socrata.datacoordinator.id.{UserColumnId, RowVersion, ColumnId}
import com.socrata.datacoordinator.truth.json.JsonColumnReadRep
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, AbstractColumnInfoLike}
import com.socrata.datacoordinator.util.collection.{MutableUserColumnIdMap, UserColumnIdMap, ColumnIdMap}
import com.socrata.soql.environment.{TypeName, ColumnName}

class RowDecodePlan[CT, CV](schema: ColumnIdMap[ColumnInfo[CT]],
                            repFor: CT => JsonColumnReadRep[CT, CV],
                            typeNameFor: CT => TypeName,
                            versionOf: CV => Option[RowVersion],
                            onUnknownColumn: UserColumnId => Unit)
  extends (JValue => Either[(CV, Option[Option[RowVersion]]), Row[CV]])
{
  sealed abstract class BadDataException(msg: String) extends Exception(msg)
  case class BadUpsertCommandException(value: JValue) extends
      BadDataException("Upsert command not an object or a singleton list")
  case class UninterpretableFieldValue(column: UserColumnId, value: JValue, columnType: CT) extends
      BadDataException(s"Unable to interpret value for field ${column} as " +
                       s"${typeNameFor(columnType)}: $value")

  val pkCol = schema.values.find(_.isUserPrimaryKey)
                           .orElse(schema.values.find(_.isSystemPrimaryKey))
                           .getOrElse { sys.error("No system primary key in the schema?") }
  val pkRep = repFor(pkCol.typ)
  val versionCol = schema.values.find(_.isVersion).getOrElse {
    sys.error("No version column in the schema?")
  }
  val versionRep = repFor(versionCol.typ)
  val cookedSchema = locally {
    val res = MutableUserColumnIdMap[(ColumnId, JsonColumnReadRep[CT, CV])]()
    schema.foreach { (systemId, ci) =>
      res(ci.userColumnId) = (systemId, repFor(ci.typ))
    }
    res.freeze()
  }

  val columnIds = locally {
    val cids = new java.util.HashMap[String, String]
    cookedSchema.keys.foreach { k =>
      cids.put(k.underlying, k.underlying)
    }
    cids
  }
  def columnId(name: String): UserColumnId =
    columnIds.get(name) match {
      case null =>
        // bad user; unknown column.  We're either going to throw or
        // ignore this, so just wrap it up and return it.
        new UserColumnId(name)
      case existingName =>
        new UserColumnId(existingName)
    }

  def cook(row: scala.collection.Map[String, JValue]): UserColumnIdMap[JValue] = {
    val res = MutableUserColumnIdMap[JValue]()
    row.foreach { case (k, v) =>
      res(columnId(k)) = v
    }
    res.freeze()
  }

  /** Turns a row data value into either `Left(id)` (if it is a delete command) or `Right(row)`
    * if it is upsert.
    * @throws RowDecodePlan.BadDataException if the data is uninterpretable
    */
  def apply(json: JValue): Either[(CV, Option[Option[RowVersion]]), Row[CV]] = json match {
    case JObject(rawRow) =>
      val row = cook(rawRow)
      val result = new MutableRow[CV]
      row.foreach { (cid, value) =>
        cookedSchema.get(cid) match {
          case Some((sid, rep)) =>
            rep.fromJValue(value) match {
              case Some(trueValue) =>
                result(sid) = trueValue
              case None =>
                throw new UninterpretableFieldValue(cid, value, rep.representedType)
            }
          case None =>
            onUnknownColumn(cid)
        }
      }
      Right(result.freeze())

    case JArray(Seq(value)) =>
      pkRep.fromJValue(value) match {
        case Some(trueValue) =>
          Left((trueValue, None))
        case None =>
          throw new UninterpretableFieldValue(pkCol.userColumnId, value, pkRep.representedType)
      }

    case JArray(Seq(value, version)) =>
      val id = pkRep.fromJValue(value) match {
        case Some(trueValue) =>
          trueValue
        case None =>
          throw new UninterpretableFieldValue(pkCol.userColumnId, value, pkRep.representedType)
      }
      val v = versionRep.fromJValue(version) match {
        case Some(trueVersion) =>
          trueVersion
        case None =>
          throw new UninterpretableFieldValue(versionCol.userColumnId, value, versionRep.representedType)
      }
      Left((id, Some(versionOf(v))))

    case other =>
      throw new BadUpsertCommandException(other)
  }
}
