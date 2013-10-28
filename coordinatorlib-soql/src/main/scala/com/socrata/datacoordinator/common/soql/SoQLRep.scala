package com.socrata.datacoordinator.common.soql

import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.datacoordinator.id.{RowVersion, RowId}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, DatasetInfo}

object SoQLRep {
  private val sqlRepFactories = Map[SoQLType, ColumnInfo[SoQLType] => SqlColumnRep[SoQLType, SoQLValue]](
    SoQLID -> (ci => new sqlreps.IDRep(ci.physicalColumnBase)),
    SoQLVersion -> (ci => new sqlreps.VersionRep(ci.physicalColumnBase)),
    SoQLText -> (ci => new sqlreps.TextRep(ci.physicalColumnBase)),
    SoQLBoolean -> (ci => new sqlreps.BooleanRep(ci.physicalColumnBase)),
    SoQLNumber -> (ci => new sqlreps.NumberLikeRep(SoQLNumber, _.asInstanceOf[SoQLNumber].value, SoQLNumber(_), ci.physicalColumnBase)),
    SoQLMoney -> (ci => new sqlreps.NumberLikeRep(SoQLNumber, _.asInstanceOf[SoQLMoney].value, SoQLMoney(_), ci.physicalColumnBase)),
    SoQLFixedTimestamp -> (ci => new sqlreps.FixedTimestampRep(ci.physicalColumnBase)),
    SoQLFloatingTimestamp -> (ci => new sqlreps.FloatingTimestampRep(ci.physicalColumnBase)),
    SoQLDate -> (ci => new sqlreps.DateRep(ci.physicalColumnBase)),
    SoQLTime -> (ci => new sqlreps.TimeRep(ci.physicalColumnBase)),
    SoQLLocation -> (ci => new sqlreps.LocationRep(ci.physicalColumnBase)),
    SoQLDouble -> (ci => new sqlreps.DoubleRep(ci.physicalColumnBase)),
    SoQLObject -> (ci => new sqlreps.ObjectRep(ci.physicalColumnBase)),
    SoQLArray -> (ci => new sqlreps.ArrayRep(ci.physicalColumnBase))
  )

  def sqlRep(columnInfo: ColumnInfo[SoQLType]): SqlColumnRep[SoQLType, SoQLValue] =
    sqlRepFactories(columnInfo.typ)(columnInfo)

  // for(typ <- SoQLType.typesByName.values) assert(repFactories.contains(typ))

  private val csvRepFactories = Map[SoQLType, CsvColumnRep[SoQLType, SoQLValue]](
    SoQLID -> csvreps.IDRep,
    SoQLText -> csvreps.TextRep,
    SoQLBoolean -> csvreps.BooleanRep,
    SoQLNumber -> new csvreps.NumberLikeRep(SoQLNumber, SoQLNumber(_)),
    SoQLMoney -> new csvreps.NumberLikeRep(SoQLMoney, SoQLMoney(_)),
    SoQLFixedTimestamp -> csvreps.FixedTimestampRep,
    SoQLFloatingTimestamp -> csvreps.FloatingTimestampRep,
    SoQLDate -> csvreps.DateRep,
    SoQLTime -> csvreps.TimeRep,
    SoQLLocation -> csvreps.LocationRep
  )
  def csvRep(columnInfo: ColumnInfo[SoQLType]): CsvColumnRep[SoQLType, SoQLValue] =
    csvRepFactories(columnInfo.typ)
  def csvRep(typ: SoQLType): CsvColumnRep[SoQLType, SoQLValue] =
    csvRepFactories(typ)

  val jsonRepFactoriesMinusIdAndVersion = Map[SoQLType, JsonColumnRep[SoQLType, SoQLValue]](
    SoQLText -> jsonreps.TextRep,
    SoQLBoolean -> jsonreps.BooleanRep,
    SoQLNumber -> new jsonreps.NumberLikeRep(SoQLNumber, _.asInstanceOf[SoQLNumber].value, SoQLNumber(_)),
    SoQLMoney -> new jsonreps.NumberLikeRep(SoQLMoney, _.asInstanceOf[SoQLMoney].value, SoQLMoney(_)),
    SoQLFixedTimestamp -> jsonreps.FixedTimestampRep,
    SoQLFloatingTimestamp -> jsonreps.FloatingTimestampRep,
    SoQLDate -> jsonreps.DateRep,
    SoQLTime -> jsonreps.TimeRep,
    SoQLLocation -> jsonreps.LocationRep,
    SoQLDouble -> jsonreps.DoubleRep,
    SoQLArray -> jsonreps.ArrayRep,
    SoQLObject -> jsonreps.ObjectRep
  )

  trait IdObfuscationContext {
    def obfuscate(rowId: RowId): String
    def deobfuscate(obfuscatedRowId: String): Option[RowId]
  }

  trait VersionObfuscationContext {
    def obfuscate(version: RowVersion): String
    def deobfuscate(obfuscatedRowVersion: String): Option[RowVersion]
  }

  private def jsonRepFactories(idStringRep: SoQLID.StringRep, versionStringRep: SoQLVersion.StringRep) =
    jsonRepFactoriesMinusIdAndVersion ++ Seq(
      SoQLID -> new jsonreps.IDRep(idStringRep),
      SoQLVersion -> new jsonreps.VersionRep(versionStringRep)
    )

  def jsonRep(idStringRep: SoQLID.StringRep, versionStringRep: SoQLVersion.StringRep): (SoQLType => JsonColumnRep[SoQLType, SoQLValue]) =
    jsonRepFactories(idStringRep, versionStringRep)
}
