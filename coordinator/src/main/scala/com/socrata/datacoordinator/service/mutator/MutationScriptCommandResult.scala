package com.socrata.datacoordinator.service.mutator

import com.socrata.soql.environment.TypeName
import scala.collection.immutable.NumericRange
import com.socrata.datacoordinator.id.UserColumnId

sealed trait MutationScriptCommandResult
object MutationScriptCommandResult {
  case class ColumnCreated(id: UserColumnId, typ: TypeName) extends MutationScriptCommandResult
  case object Uninteresting extends MutationScriptCommandResult
  case class RowData(results: NumericRange[Long]) extends MutationScriptCommandResult
}
