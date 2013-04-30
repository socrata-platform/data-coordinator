package com.socrata.datacoordinator.common.soql

import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.datacoordinator.id.RowId
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

  private val jsonRepFactoriesMinusId = Map[SoQLType, ColumnInfo[SoQLType] => JsonColumnRep[SoQLType, SoQLValue]](
    SoQLText -> (ci => new jsonreps.TextRep(ci.logicalName)),
    SoQLBoolean -> (ci => new jsonreps.BooleanRep(ci.logicalName)),
    SoQLNumber -> (ci => new jsonreps.NumberLikeRep(ci.logicalName, SoQLNumber, _.asInstanceOf[SoQLNumber].value, SoQLNumber(_))),
    SoQLMoney -> (ci => new jsonreps.NumberLikeRep(ci.logicalName, SoQLMoney, _.asInstanceOf[SoQLMoney].value, SoQLMoney(_))),
    SoQLFixedTimestamp -> (ci => new jsonreps.FixedTimestampRep(ci.logicalName)),
    SoQLFloatingTimestamp -> (ci => new jsonreps.FloatingTimestampRep(ci.logicalName)),
    SoQLDate -> (ci => new jsonreps.DateRep(ci.logicalName)),
    SoQLTime -> (ci => new jsonreps.TimeRep(ci.logicalName)),
    SoQLLocation -> (ci => new jsonreps.LocationRep(ci.logicalName)),
    SoQLDouble -> (ci => new jsonreps.DoubleRep(ci.logicalName)),
    SoQLArray -> (ci => new jsonreps.ArrayRep(ci.logicalName)),
    SoQLObject -> (ci => new jsonreps.ObjectRep(ci.logicalName))
  )

  trait IdObfuscationContext {
    def obfuscate(rowId: RowId): String
    def deobfuscate(obfuscatedRowId: String): Option[RowId]
  }

  private def jsonRepFactories(obfuscationContext: DatasetInfo => IdObfuscationContext) =
    jsonRepFactoriesMinusId ++ Seq(
      SoQLID -> ((ci: ColumnInfo[SoQLType]) => new jsonreps.IDRep(ci.logicalName, obfuscationContext(ci.copyInfo.datasetInfo))),
      SoQLVersion -> ((ci: ColumnInfo[SoQLType]) => new jsonreps.VersionRep(ci.logicalName, obfuscationContext(ci.copyInfo.datasetInfo)))
    )

  def jsonRep(obfuscationContext: DatasetInfo => IdObfuscationContext): (ColumnInfo[SoQLType] => JsonColumnRep[SoQLType, SoQLValue]) = {
    val factories = jsonRepFactories(obfuscationContext);
    { ci => factories(ci.typ)(ci) }
  }
}
