package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.util.RotateSchema

class DatasetCopyContext[CT](val copyInfo: CopyInfo, val schema: ColumnIdMap[ColumnInfo[CT]]) {
  require(schema.values.forall(_.copyInfo eq copyInfo))
  def datasetInfo = copyInfo.datasetInfo

  lazy val schemaByLogicalName = RotateSchema(schema)
  lazy val userIdCol = schema.values.find(_.isUserPrimaryKey)
  lazy val systemIdCol = schema.values.find(_.isSystemPrimaryKey)
  def systemIdCol_! = systemIdCol.getOrElse {
    sys.error("No system PK defined on this dataset?")
  }
  lazy val versionCol = schema.values.find(_.isVersion)
  lazy val versionCol_! = versionCol.getOrElse {
    sys.error("No version column defined on this dataset?")
  }
  lazy val pkCol = userIdCol.orElse(systemIdCol)
  lazy val pkCol_! = userIdCol.getOrElse(systemIdCol_!)

  def verticalSlice(f: ColumnInfo[CT] => Boolean) = new DatasetCopyContext(copyInfo, schema.filter { case (_, col) => f(col) })

  def thaw() = new MutableDatasetCopyContext[CT](copyInfo, new MutableColumnIdMap(schema))
}

class MutableDatasetCopyContext[CT](var _copyInfo: CopyInfo, private var schema: MutableColumnIdMap[ColumnInfo[CT]]) {
  def datasetInfo = _copyInfo.datasetInfo
  var _currentSchema: ColumnIdMap[ColumnInfo[CT]] = null
  def copyInfo = _copyInfo
  def copyInfo_=(newCopyInfo: CopyInfo) {
    require(_copyInfo.datasetInfo.systemId == newCopyInfo.datasetInfo.systemId)
    val newSchema = new MutableColumnIdMap(schema)
    for(col <- schema.values) {
      newSchema(col.systemId) = col.copy(copyInfo = newCopyInfo)(col.typeNamespace, null)
    }
    schema = newSchema
    _copyInfo = newCopyInfo
    _currentSchema = null
  }
  def columnInfoOpt(id: ColumnId) = schema.get(id)
  def columnInfoOpt(name: ColumnName) = schema.values.find(_.logicalName == name) // yeah, O(n) in the number of columns...

  def columnInfo(id: ColumnId) = schema(id)
  def columnInfo(name: ColumnName) = columnInfoOpt(name).getOrElse {
    throw new NoSuchElementException(name.name)
  }

  def addColumn(newColumnInfo: ColumnInfo[CT]) {
    // We are now either ADDING A NEW COLUMN or REPLACING AN EXISTING ONE.
    // Both cases are distinguished by the column's SYSTEM ID.
    // In the former case, a column with the same name MUST NOT EXIST.
    // In the latter case, it is allowed to exist IF IT IS THE COLUMN BEING REPLACED.
    columnInfoOpt(newColumnInfo.systemId) match {
      case None =>
        require(columnInfoOpt(newColumnInfo.logicalName) == None)
      case Some(oldCol) =>
        require(oldCol.logicalName == newColumnInfo.logicalName || columnInfoOpt(newColumnInfo.logicalName) == None)
    }
    schema(newColumnInfo.systemId) = newColumnInfo
    _currentSchema = null
  }

  def currentColumns = schema.values.toVector

  def removeColumn(cid: ColumnId) {
    schema -= cid
    _currentSchema = null
  }

  def currentSchema = {
    if(_currentSchema == null) _currentSchema = schema.frozenCopy()
    _currentSchema
  }

  def freeze() = new DatasetCopyContext(copyInfo, schema.freeze())
  def frozenCopy() = new DatasetCopyContext(copyInfo, schema.frozenCopy())
}
