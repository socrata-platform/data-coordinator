package com.socrata.datacoordinator
package service

import com.socrata.datacoordinator.truth.json.JsonColumnReadRep
import com.rojoma.json.ast.{JArray, JBoolean, JObject, JValue}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, AbstractColumnInfoLike}
import com.socrata.datacoordinator.id.{RowVersion, ColumnId}
import com.socrata.soql.environment.{TypeName, ColumnName}

class RowDecodePlan[CT, CV](schema: ColumnIdMap[ColumnInfo[CT]], repFor: CT => JsonColumnReadRep[CT, CV], typeNameFor: CT => TypeName, versionOf: CV => Option[RowVersion])
  extends (JValue => Either[(CV, Option[Option[RowVersion]]), Row[CV]])
{
  sealed abstract class BadDataException(msg: String) extends Exception(msg)
  case class BadUpsertCommandException(value: JValue) extends BadDataException("Upsert command not an object or a singleton list")
  case class UninterpretableFieldValue(column: ColumnName, value: JValue, columnType: CT) extends BadDataException("Unable to interpret value for field " + column + " as " + typeNameFor(columnType) + ": " + value)

  val pkCol = schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
    sys.error("No system primary key in the schema?")
  }
  val pkRep = repFor(pkCol.typ)
  val versionCol = schema.values.find(_.isVersion).getOrElse {
    sys.error("No version column in the schema?")
  }
  val versionRep = repFor(versionCol.typ)
  val cookedSchema: Array[(ColumnName, ColumnId, JsonColumnReadRep[CT, CV])] =
    schema.iterator.map { case (systemId, ci) =>
      (ci.logicalName, systemId, repFor(ci.typ))
    }.toArray

  val columnNames = new java.util.HashMap[String, ColumnName]
  def columnName(name: String): ColumnName =
    columnNames.get(name) match {
      case null =>
        if(columnNames.size > 2000) columnNames.clear() // bad user!
        val newName = ColumnName(name)
        columnNames.put(name, newName)
        newName
      case existingName =>
        existingName
    }

  def cook(row: scala.collection.Map[String, JValue]): Map[ColumnName, JValue] = {
    row.foldLeft(Map.empty[ColumnName, JValue]) { (acc, kv) =>
      acc + (columnName(kv._1) -> kv._2)
    }
  }

  /** Turns a row data value into either `Left(id)` (if it is a delete command) or `Right(row)`
    * if it is upsert.
    * @throws RowDecodePlan.BadDataException if the data is uninterpretable
    */
  def apply(json: JValue): Either[(CV, Option[Option[RowVersion]]), Row[CV]] = json match {
    case JObject(rawRow) =>
      val row = cook(rawRow)
      val result = new MutableRow[CV]
      cookedSchema.foreach { case (field, systemId, rep) =>
        row.get(field) match {
          case Some(value) =>
            rep.fromJValue(value) match {
              case Some(trueValue) =>
                result(systemId) = trueValue
              case None =>
                throw new UninterpretableFieldValue(field, value, rep.representedType)
            }
          case None =>
          /* pass */
        }
      }
      Right(result.freeze())
    case JArray(Seq(value)) =>
      pkRep.fromJValue(value) match {
        case Some(trueValue) =>
          Left((trueValue, None))
        case None =>
          throw new UninterpretableFieldValue(pkCol.logicalName, value, pkRep.representedType)
      }
    case JArray(Seq(value, version)) =>
      val id = pkRep.fromJValue(value) match {
        case Some(trueValue) =>
          trueValue
        case None =>
          throw new UninterpretableFieldValue(pkCol.logicalName, value, pkRep.representedType)
      }
      val v = versionRep.fromJValue(version) match {
        case Some(trueVersion) =>
          trueVersion
        case None =>
          throw new UninterpretableFieldValue(versionCol.logicalName, value, versionRep.representedType)
      }
      Left((id, Some(versionOf(v))))
    case other =>
      throw new BadUpsertCommandException(other)
  }
}
