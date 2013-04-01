package com.socrata.datacoordinator
package service

import com.socrata.datacoordinator.truth.json.JsonColumnReadRep
import com.rojoma.json.ast.{JBoolean, JObject, JValue}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.AbstractColumnInfoLike
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.soql.environment.ColumnName

class RowDecodePlan[CT, CV](schema: ColumnIdMap[AbstractColumnInfoLike], repFor: AbstractColumnInfoLike => JsonColumnReadRep[CT, CV], magicDeleteKey: ColumnName)
  extends (JValue => Either[CV, Row[CV]])
{
  val pkCol = schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
    sys.error("No system primary key in the schema?")
  }
  val pkId = pkCol.logicalName
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

  def apply(json: JValue): Either[CV, Row[CV]] = json match {
    case JObject(rawRow) =>
      val row = cook(rawRow)
      if(row.contains(magicDeleteKey) && JBoolean.canonicalTrue == row(magicDeleteKey)) {
        row.get(pkId) match {
          case Some(value) =>
            pkRep.fromJValue(value) match {
              case Some(trueValue) =>
                Left(trueValue)
              case None =>
                ??? // TODO throw new BadDataException("Unable to interpret field " + pkId)
            }
          case None =>
            ??? // TODO throw new BadDataException("Delete command without associated ID value")
        }
      } else {
        val result = new MutableRow[CV]
        cookedSchema.foreach { case (field, systemId, rep) =>
          row.get(field) match {
            case Some(value) =>
              rep.fromJValue(value) match {
                case Some(trueValue) =>
                  result(systemId) = trueValue
                case None =>
                  ??? // TODO throw new BadDataException("Unable to interpret field " + field)
              }
            case None =>
              /* pass */
          }
        }
        Right(result.freeze())
      }
    case _ =>
      ??? // TODO: bad data
  }
}
