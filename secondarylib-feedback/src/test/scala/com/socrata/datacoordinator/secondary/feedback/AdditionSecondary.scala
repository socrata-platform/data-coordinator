package com.socrata.datacoordinator.secondary.feedback

import com.rojoma.json.v3.ast.JValue
import com.socrata.datacoordinator.id.{StrategyType, ColumnId, UserColumnId}
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.secondary.feedback.instance.{SoQLTypeFromJValue, SoQLTypeFor, SoQLValueRepFrom, SoQLValueRepFor, SoQLEstimateSize}
import com.socrata.datacoordinator.secondary.feedback.monitor.{DummyStatusMonitor, StatusMonitor}
import com.socrata.soql.types._


case class AdditionColumnInfo(sources: Seq[ColumnId], strategy: ComputationStrategyInfo, targetColId: UserColumnId) extends HasStrategy
case class AdditionRowInfo(data: secondary.Row[SoQLValue], sources: Seq[ColumnId], targetColId: UserColumnId)

class AdditionHandler extends ComputationHandler[SoQLType, SoQLValue] {
  type PerDatasetData = CookieSchema
  type PerColumnData = AdditionColumnInfo
  type PerCellData = AdditionRowInfo

  override def matchesStrategyType(typ: StrategyType): Boolean = typ.underlying == "addition"

  override def setupDataset(cookie: CookieSchema): CookieSchema = cookie

  override def setupColumn(cookie: CookieSchema, strategy: ComputationStrategyInfo, targetColId: UserColumnId): AdditionColumnInfo = {
    new AdditionColumnInfo(strategy.sourceColumnIds.map(cookie.columnIdMap(_)), strategy, targetColId)
  }

  override def setupCell(colInfo: AdditionColumnInfo, row: Row[SoQLValue]): AdditionRowInfo = {
    AdditionRowInfo(row.data, colInfo.sources, colInfo.targetColId)
  }

  override def compute[RowHandle](sources: Map[RowHandle, Seq[AdditionRowInfo]]): ComputationResult[RowHandle] = {
    val res = sources.map { case (handle, rows) =>
      val resultRows = rows.map { rowInfo =>
        val values = rowInfo.data.filter { case (colId, _) => rowInfo.sources.contains(colId)}.values
        val result = values.foldLeft(java.math.BigDecimal.ZERO) { case (sum, value) =>
          value match {
            case SoQLNumber(number) => sum.add(number)
            case SoQLNull => sum
            case other => return Left(FatalComputationError(s"Unexpected type: ${other.typ}"))
          }
        }
        (rowInfo.targetColId, SoQLNumber(result))
      }
      (handle, resultRows.toMap)
    }
    Right[ComputationFailure, Map[RowHandle, Map[UserColumnId, SoQLValue]]](res)
  }
}

class AdditionSecondary(dataCoordinatorClient: DataCoordinatorClient[SoQLType, SoQLValue])
  extends FeedbackSecondary[SoQLType, SoQLValue] {

  override val user = "addition-secondary"

  // The tests want predictable batch sizes.  This is a _little_
  // difficult, but what we'll do here is: insert operations
  // guesstimate at size 1, and update operations (which have "old
  // data") will guestimate at size 2 (and hence actually batch into
  // groups of 3).
  override val baseBatchSize: Int = 5
  override val estimateValueSize = { (v: SoQLValue) => v match { case id: SoQLID => 1; case _ => 0 } }

  override val computationHandlers =  Seq(new AdditionHandler)
  override val computationRetryLimit = 5

  override def dataCoordinator = { case (_ : String, _ : (SoQLType => JValue => Option[SoQLValue])) =>
    dataCoordinatorClient
  }
  override val dataCoordinatorRetryLimit: Int = 5

  override val repFor = SoQLValueRepFor
  override val repFrom = SoQLValueRepFrom

  override val typeFor = SoQLTypeFor
  override val typeFromJValue = SoQLTypeFromJValue

  override val statusMonitor: StatusMonitor = new DummyStatusMonitor

  override def shutdown(): Unit = {}
}
