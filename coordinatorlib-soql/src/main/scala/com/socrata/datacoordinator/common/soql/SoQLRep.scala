package com.socrata.datacoordinator.common.soql

import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.id.RowId

object SoQLRep {
  val sqlRepFactories = Map[SoQLType, String => SqlColumnRep[SoQLType, Any]](
    SoQLID -> (base => new sqlreps.IDRep(base)),
    SoQLText -> (base => new sqlreps.TextRep(base)),
    SoQLBoolean -> (base => new sqlreps.BooleanRep(base)),
    SoQLNumber -> (base => new sqlreps.NumberLikeRep(SoQLNumber, base)),
    SoQLMoney -> (base => new sqlreps.NumberLikeRep(SoQLNumber, base)),
    SoQLFixedTimestamp -> (base => new sqlreps.FixedTimestampRep(base)),
    SoQLFloatingTimestamp -> (base => new sqlreps.FloatingTimestampRep(base)),
    SoQLDate -> (base => new sqlreps.DateRep(base)),
    SoQLTime -> (base => new sqlreps.TimeRep(base)),
    SoQLLocation -> (base => new sqlreps.LocationRep(base)) /*,
    SoQLDouble -> doubleRepFactory,
    SoQLObject -> objectRepFactory,
    SoQLArray -> arrayRepFactory */
  )

  // for(typ <- SoQLType.typesByName.values) assert(repFactories.contains(typ))

  val csvRepFactories = Map[SoQLType, CsvColumnRep[SoQLType, Any]](
    SoQLID -> csvreps.IDRep,
    SoQLText -> csvreps.TextRep,
    SoQLBoolean -> csvreps.BooleanRep,
    SoQLNumber -> new csvreps.NumberLikeRep(SoQLNumber),
    SoQLMoney -> new csvreps.NumberLikeRep(SoQLMoney),
    SoQLFixedTimestamp -> csvreps.FixedTimestampRep,
    SoQLFloatingTimestamp -> csvreps.FloatingTimestampRep,
    SoQLDate -> csvreps.DateRep,
    SoQLTime -> csvreps.TimeRep,
    SoQLLocation -> csvreps.LocationRep
  )

  private val jsonRepFactoriesMinusId = Map[SoQLType, ColumnName => JsonColumnRep[SoQLType, Any]](
    SoQLText -> (name => new jsonreps.TextRep(name)),
    SoQLBoolean -> (name => new jsonreps.BooleanRep(name)),
    SoQLNumber -> (name => new jsonreps.NumberLikeRep(name, SoQLNumber)),
    SoQLMoney -> (name => new jsonreps.NumberLikeRep(name, SoQLMoney)),
    SoQLFixedTimestamp -> (name => new jsonreps.FixedTimestampRep(name)),
    SoQLFloatingTimestamp -> (name => new jsonreps.FloatingTimestampRep(name)),
    SoQLDate -> (base => new jsonreps.DateRep(base)),
    SoQLTime -> (base => new jsonreps.TimeRep(base)),
    SoQLLocation -> (name => new jsonreps.LocationRep(name))
  )

  trait IdObfuscationContext {
    def obfuscate(rowId: RowId): String
    def deobfuscate(obfuscatedRowId: String): Option[RowId]
  }

  def jsonRepFactories(obfuscationContext: IdObfuscationContext) =
    jsonRepFactoriesMinusId + (SoQLID -> ((name: ColumnName) => new jsonreps.IDRep(name, obfuscationContext)))
}
