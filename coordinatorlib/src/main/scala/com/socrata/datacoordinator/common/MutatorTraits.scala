package com.socrata.datacoordinator.common

import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.soql.types.ErasedCJsonRep
import com.socrata.datacoordinator.util.collection.UserColumnIdMap
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.TypeContext

trait MutatorCommon[CT, CV] {
  def physicalColumnBaseBase(nameHint: String, systemColumn: Boolean = false): String
  def isSystemColumnId(identifier: UserColumnId): Boolean
  def systemSchema: UserColumnIdMap[MutatorColumnInfo[CT]]
  def systemIdColumnId: UserColumnId
  def versionColumnId: UserColumnId
  def jsonReps(di: DatasetInfo): CT => ErasedCJsonRep[CV]
  def allowDdlOnPublishedCopies: Boolean
  def typeContext: TypeContext[CT, CV]
  def genUserColumnId(): UserColumnId
}

trait MutatorColumnInfo[CT] {
  def typ: CT
  def fieldName: Option[ColumnName]
  def computationStrategy: Option[ComputationStrategyInfo]
}
