package com.socrata.datacoordinator
package service

import com.socrata.datacoordinator.truth.json.JsonColumnReadRep
import com.rojoma.json.ast.{JBoolean, JObject, JValue}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.AbstractColumnInfoLike
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.soql.environment.ColumnName

object RowDecodePlan {
  class BadDataException(msg: String) extends Exception(msg)
  class RowNotObjectException extends BadDataException("Row data not an object")
  class UninterpretableField(val column: ColumnName, val value: JValue) extends BadDataException("Unable to interpret value for field " + column + ": " + value)
}

class RowDecodePlan[CT, CV](schema: ColumnIdMap[AbstractColumnInfoLike], repFor: AbstractColumnInfoLike => JsonColumnReadRep[CT, CV], magicDeleteKey: ColumnName)
  extends (JValue => Either[CV, Row[CV]])
{
  import RowDecodePlan._

  val pkCol = schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
    sys.error("No system primary key in the schema?")
  }
  val pkRep = repFor(pkCol)
  val cookedSchema: Array[(ColumnName, ColumnId, JsonColumnReadRep[CT, CV])] =
    schema.iterator.map { case (systemId, ci) =>
      (ci.logicalName, systemId, repFor(ci))
    }.toArray

  def cook(row: scala.collection.Map[String, JValue]): Map[ColumnName, JValue] = {
    row.foldLeft(Map.empty[ColumnName, JValue]) { (acc, kv) =>
      acc + (ColumnName(kv._1) -> kv._2)
    }
  }

  /** Turns a row data value into either `Left(id)` (if it is a delete command) or `Right(row)`
    * if it is upsert.
    * @throws RowDecodePlan.BadDataException if the data is uninterpretable
    */
  def apply(json: JValue): Either[CV, Row[CV]] = json match {
    case JObject(rawRow) =>
      val row = cook(rawRow)
      row.get(magicDeleteKey) match {
        case None =>
          val result = new MutableRow[CV]
          cookedSchema.foreach { case (field, systemId, rep) =>
            row.get(field) match {
              case Some(value) =>
                rep.fromJValue(value) match {
                  case Some(trueValue) =>
                    result(systemId) = trueValue
                  case None =>
                    throw new UninterpretableField(field, value)
                }
              case None =>
                /* pass */
            }
          }
          Right(result.freeze())
        case Some(value) =>
          pkRep.fromJValue(value) match {
            case Some(trueValue) =>
              Left(trueValue)
            case None =>
              throw new UninterpretableField(magicDeleteKey, value)
          }
      }
    case _ =>
      throw new RowNotObjectException
  }
}
