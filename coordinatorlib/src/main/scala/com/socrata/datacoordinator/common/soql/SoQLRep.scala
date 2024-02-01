package com.socrata.datacoordinator.common.soql

import com.socrata.datacoordinator.common.soql.sqlreps._
import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.datacoordinator.id.{RowId, RowVersion}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, DatasetInfo}
import com.vividsolutions.jts.geom._

object SoQLRep {
  private val sqlRepFactories = Map[SoQLType, ColumnInfo[SoQLType] => SqlColumnRep[SoQLType, SoQLValue]](
    SoQLID -> (ci => new sqlreps.IDRep(ci.physicalColumnBase)),
    SoQLVersion -> (ci => new sqlreps.VersionRep(ci.physicalColumnBase)),
    SoQLText -> (ci => new sqlreps.TextRep(ci.physicalColumnBase)),
    SoQLBoolean -> (ci => new sqlreps.BooleanRep(ci.physicalColumnBase)),
    SoQLNumber -> (ci => new sqlreps.NumberLikeRep(SoQLNumber, _.asInstanceOf[SoQLNumber].value,
                                                   SoQLNumber(_), ci.physicalColumnBase)),
    SoQLMoney -> (ci => new sqlreps.NumberLikeRep(SoQLMoney, _.asInstanceOf[SoQLMoney].value,
                                                  SoQLMoney(_), ci.physicalColumnBase)),
    SoQLFixedTimestamp -> (ci => new sqlreps.FixedTimestampRep(ci.physicalColumnBase)),
    SoQLFloatingTimestamp -> (ci => new sqlreps.FloatingTimestampRep(ci.physicalColumnBase)),
    SoQLDate -> (ci => new sqlreps.DateRep(ci.physicalColumnBase)),
    SoQLTime -> (ci => new sqlreps.TimeRep(ci.physicalColumnBase)),
    SoQLDouble -> (ci => new sqlreps.DoubleRep(ci.physicalColumnBase)),
    SoQLObject -> (ci => new sqlreps.ObjectRep(ci.physicalColumnBase)),
    SoQLArray -> (ci => new sqlreps.ArrayRep(ci.physicalColumnBase)),
    SoQLPoint -> (ci => new sqlreps.GeometryLikeRep[Point](
                    SoQLPoint,
                    _.asInstanceOf[SoQLPoint].value,
                    SoQLPoint(_),
                    ci.presimplifiedZoomLevels,
                    ci.physicalColumnBase)),
    SoQLMultiPoint -> (ci => new sqlreps.GeometryLikeRep[MultiPoint](
                         SoQLMultiPoint,
                         _.asInstanceOf[SoQLMultiPoint].value,
                         SoQLMultiPoint(_),
                         ci.presimplifiedZoomLevels,
                         ci.physicalColumnBase)),
    SoQLLine -> (ci => new GeometryLikeRep[LineString](
                   SoQLLine,
                   _.asInstanceOf[SoQLLine].value,
                   SoQLLine(_),
                   ci.presimplifiedZoomLevels,
                   ci.physicalColumnBase)),
    SoQLMultiLine -> (ci => new sqlreps.GeometryLikeRep[MultiLineString](
                        SoQLMultiLine,
                        _.asInstanceOf[SoQLMultiLine].value,
                        SoQLMultiLine(_),
                        ci.presimplifiedZoomLevels,
                        ci.physicalColumnBase)),
    SoQLPolygon -> (ci => new GeometryLikeRep[Polygon](
                      SoQLPolygon,
                      _.asInstanceOf[SoQLPolygon].value,
                      SoQLPolygon(_),
                      ci.presimplifiedZoomLevels,
                      ci.physicalColumnBase)),
    SoQLMultiPolygon -> (ci => new sqlreps.GeometryLikeRep[MultiPolygon](
                           SoQLMultiPolygon,
                           _.asInstanceOf[SoQLMultiPolygon].value,
                           SoQLMultiPolygon(_),
                           ci.presimplifiedZoomLevels,
                           ci.physicalColumnBase)),
    SoQLLocation -> (ci => new LocationRep(ci.physicalColumnBase)),
    SoQLPhone -> (ci => new PhoneRep(ci.physicalColumnBase)),
    SoQLUrl -> (ci => new UrlRep(ci.physicalColumnBase)),
    SoQLDocument -> (ci => new DocumentRep(ci.physicalColumnBase)),
    SoQLPhoto -> (ci => new PhotoRep(ci.physicalColumnBase)),
    SoQLBlob -> (ci => new sqlreps.BlobRep(ci.physicalColumnBase)),
    SoQLJson -> (ci => new sqlreps.JsonRep(ci.physicalColumnBase))
  )

  def sqlRep(columnInfo: ColumnInfo[SoQLType]): SqlColumnRep[SoQLType, SoQLValue] =
    sqlRepFactories(columnInfo.typ)(columnInfo)

  // for(typ <- SoQLType.typesByName.values) assert(repFactories.contains(typ))

  private val csvReps = Map[SoQLType, CsvColumnRep[SoQLType, SoQLValue]](
    SoQLID -> csvreps.IDRep,
    SoQLText -> csvreps.TextRep,
    SoQLBoolean -> csvreps.BooleanRep,
    SoQLNumber -> new csvreps.NumberLikeRep(SoQLNumber, SoQLNumber(_)),
    SoQLMoney -> new csvreps.NumberLikeRep(SoQLMoney, SoQLMoney(_)),
    SoQLFixedTimestamp -> csvreps.FixedTimestampRep,
    SoQLFloatingTimestamp -> csvreps.FloatingTimestampRep,
    SoQLDate -> csvreps.DateRep,
    SoQLTime -> csvreps.TimeRep,
    SoQLPoint -> new csvreps.GeometryLikeRep[Point](SoQLPoint, SoQLPoint(_)),
    SoQLMultiPoint -> new csvreps.GeometryLikeRep[MultiPoint](SoQLMultiPoint, SoQLMultiPoint(_)),
    SoQLLine -> new csvreps.GeometryLikeRep[LineString](SoQLLine, SoQLLine(_)),
    SoQLMultiLine -> new csvreps.GeometryLikeRep[MultiLineString](SoQLMultiLine, SoQLMultiLine(_)),
    SoQLPolygon -> new csvreps.GeometryLikeRep[Polygon](SoQLPolygon, SoQLPolygon(_)),
    SoQLMultiPolygon -> new csvreps.GeometryLikeRep[MultiPolygon](SoQLMultiPolygon, SoQLMultiPolygon(_)),
    SoQLLocation -> csvreps.LocationRep,
    SoQLPhone -> csvreps.PhoneRep,
    SoQLUrl -> csvreps.UrlRep,
    SoQLDocument -> csvreps.DocumentRep,
    SoQLPhoto -> csvreps.PhotoRep,
    SoQLBlob -> csvreps.BlobRep,
    SoQLJson -> csvreps.JsonRep
  )
  def csvRep(columnInfo: ColumnInfo[SoQLType]): CsvColumnRep[SoQLType, SoQLValue] =
    csvReps(columnInfo.typ)
  def csvRep(typ: SoQLType): CsvColumnRep[SoQLType, SoQLValue] =
    csvReps(typ)

  val jsonRepsMinusIdAndVersion: Map[SoQLType, ErasedCJsonRep[SoQLValue]] =
    SoQLType.allTypes.iterator.collect {
      case t: SoQLType with NonObfuscatedType => t -> t.cjsonRep.asErasedCJsonRep
    }.toMap

  trait IdObfuscationContext {
    def obfuscate(rowId: RowId): String
    def deobfuscate(obfuscatedRowId: String): Option[RowId]
  }

  trait VersionObfuscationContext {
    def obfuscate(version: RowVersion): String
    def deobfuscate(obfuscatedRowVersion: String): Option[RowVersion]
  }

  private def jsonRepFactories(cryptProvider: obfuscation.CryptProvider, obfuscateIds: Boolean) =
    jsonRepsMinusIdAndVersion ++ Seq(
      SoQLID -> SoQLID.cjsonRep(cryptProvider, obfuscateIds).asErasedCJsonRep,
      SoQLVersion -> SoQLVersion.cjsonRep(cryptProvider).asErasedCJsonRep
    )

  def jsonRep(cryptProvider: obfuscation.CryptProvider, obfuscateIds: Boolean): (SoQLType => ErasedCJsonRep[SoQLValue]) =
    jsonRepFactories(cryptProvider, obfuscateIds)
}
