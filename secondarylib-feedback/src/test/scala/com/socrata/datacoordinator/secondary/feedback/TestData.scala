package com.socrata.datacoordinator.secondary.feedback

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.interpolation._
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import org.joda.time.DateTime

object TestData {
  val datasetInfo = DatasetInfo("test", "en_us", "magicmagic".getBytes, Some("test"))

  val systemPK = ColumnInfo[SoQLType](new ColumnId(1), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID,
    isSystemPrimaryKey = true, isUserPrimaryKey = false, isVersion = false, None)

  val num1 = ColumnInfo[SoQLType](new ColumnId(2), new UserColumnId("num_1"), Some(ColumnName("num_1")), SoQLNumber,
    isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false, None)

  val num2 = ColumnInfo[SoQLType](new ColumnId(3), new UserColumnId("num_2"), Some(ColumnName("num_2")), SoQLNumber,
    isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false, None)

  val sum12 = ColumnInfo[SoQLType](new ColumnId(4), new UserColumnId("sum_1_2"), Some(ColumnName("sum_1_2")), SoQLNumber,
    isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false,
    Some(ComputationStrategyInfo(StrategyType("addition"), Seq(num1.id, num2.id), JObject.canonicalEmpty)))

  val num3 = ColumnInfo[SoQLType](new ColumnId(5), new UserColumnId("num_3"), Some(ColumnName("num_3")), SoQLNumber,
    isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false, None)

  val sum23 = ColumnInfo[SoQLType](new ColumnId(6), new UserColumnId("sum_2_3"), Some(ColumnName("sum_2_3")), SoQLNumber,
    isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false,
    Some(ComputationStrategyInfo(StrategyType("addition"), Seq(num2.id, num3.id), JObject.canonicalEmpty)))

  val schema = ColumnIdMap(
    systemPK.systemId -> systemPK,
    num2.systemId -> num2,
    num3.systemId -> num3,
    sum23.systemId -> sum23
  )

  def v7copyInfo = CopyInfo(new CopyId(10), 1, LifecycleStage.Published, 7, DateTime.now)
  def v8copyInfo = CopyInfo(new CopyId(10), 2, LifecycleStage.Unpublished, 8, DateTime.now)

  def v10copyInfo = CopyInfo(new CopyId(10), 1, LifecycleStage.Published, 10, DateTime.now)
  def v11copyInfo = CopyInfo(new CopyId(10), 1, LifecycleStage.Published, 11, DateTime.now)

  object TestRows {
    type ExportRowsResult = Either[RequestFailure, Either[ColumnsDoNotExist, RowData[SoQLValue]]]


    val systemPKRows = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9).map(SoQLID(_))

    def toSoQLNumbers(it: Iterable[Int]): Iterable[SoQLNumber] = it.map { num =>
      SoQLNumber(new java.math.BigDecimal(num))
    }

    val num1Rows = toSoQLNumbers(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val num2Rows = toSoQLNumbers(Seq(1, 3, 5, 7, 9, 11, 13, 15, 17))

    val num3RowsV6 = num1Rows.map(_ => SoQLNull)
    val num3RowsV9 = num3RowsV6
    val num3RowsV10 = num1Rows

    val sum12Rows = Seq(2, 5, 8, 11, 14, 17, 20, 23, 26)

    val sum23RowsV6 = num2Rows
    val sum23RowsV9 = sum23RowsV6
    val sum23RowsV11 = sum12Rows

    def toRows[CV <: SoQLValue](pk: ColumnInfo[_ <: SoQLType],
                                pkRows: Iterable[CV],
                                rowMap: Map[ColumnInfo[_ <: SoQLType], Iterable[CV]]): Iterator[secondary.Row[CV]] = {
      val columns = rowMap.map { case (col, values) => (col.systemId, values.toIndexedSeq) }
      pkRows.zipWithIndex.map { case (pkVal, index) =>
        ColumnIdMap(Map(pk.systemId -> pkVal) ++ columns.map { case (col, values) => (col, values(index)) }.toMap)
      }.toIterator
    }

    def num1Num2Rows = toRows(systemPK, systemPKRows, Map(num1 -> num1Rows, num2 -> num2Rows))

    def managedRows(rows: Iterator[ColumnIdMap[SoQLValue]]) = new SimpleArm[Iterator[ColumnIdMap[SoQLValue]]] {
      override def flatMap[B](f: (Iterator[ColumnIdMap[SoQLValue]]) => B): B = f(rows)
    }

    def num2Num3Sum23RowsV7 = toRows(systemPK, systemPKRows, Map(num2 -> num2Rows, num3 -> num3RowsV6, sum23 -> sum23RowsV6))
    def num2Num3Sum23RowsV10 = toRows(systemPK, systemPKRows, Map(num2 -> num2Rows, num3 -> num3RowsV10, sum23 -> sum23RowsV9))
    def num2Num3Sum23RowsV11 = toRows(systemPK, systemPKRows, Map(num2 -> num2Rows, num3 -> num3RowsV10, sum23 -> toSoQLNumbers(sum23RowsV11)))

    // successes
    val systemPKEmpty: ExportRowsResult = Right(Right(RowData(systemPK.id, Iterator.empty)))
    def systemPKNum1Num2: ExportRowsResult = Right(Right(RowData(systemPK.id, num1Num2Rows)))
    val num1DoesNotExist: ExportRowsResult = Right(Left(ColumnsDoNotExist(Set(num1.id))))

    // failures
    val unexpectedError: ExportRowsResult = Left(UnexpectedError("test", null))
    val failedToDiscoverDC: ExportRowsResult = Left(FailedToDiscoverDataCoordinator)
    val dataCoordinatorBusy: ExportRowsResult = Left(DataCoordinatorBusy)
    val doesNotExist: ExportRowsResult = Left(DatasetDoesNotExist)

  }

  object TestScripts {
    import TestRows._

    val commands: Seq[JValue] =
      j"""[ { "c" : "normal", "user" : "addition-secondary" }
        , { "c" : "row data", "update_only" : true, "nonfatal_row_errors" : [ "insert_in_update_only", "no_such_row_to_update" ] }
        ]""".toSeq

    val batchSize = 5

    val systemPKObsRows = Seq(
      "row-jcqm-z7ug.gem5",
      "row-v9jc.hw3f_32uh",
      "row-iz2a-yf4p.gisa",
      "row-sixc.zigb~8kwk",
      "row-79ir.pmda.3as5",
      "row-nggb.aqc3_aeth",
      "row-4392_5qy3-fkfy",
      "row-z9wk.pbyx_kn4b",
      "row-7vyr.88y6.a384"
    ).map(JString(_))

    def toScripts(pk: ColumnInfo[_ <: SoQLType],
                  pkRows: Iterable[JValue],
                  rowMap: Map[ColumnInfo[_ <: SoQLType], Iterable[JValue]]): Array[JArray] = {
      val columns = rowMap.map { case (col, values) => (col.id.underlying, values.toIndexedSeq) }
      pkRows.zipWithIndex.map { case (pkVal, index) =>
        JObject(Map(pk.id.underlying -> pkVal) ++ columns.map { case (col, values) => (col, values(index)) }.toMap)
      }.grouped(batchSize).map { batch =>
        JArray(commands ++ batch)
      }.toArray
    }

    val sum12Scripts = toScripts(systemPK, systemPKObsRows, Map(sum12 -> sum12Rows.map(JNumber(_))))
    val sum23ScriptsV11 = toScripts(systemPK, systemPKObsRows, Map(sum23 -> sum23RowsV11.map(JNumber(_))))

    // DataCoordinatorClient.postMutationScriptResult(.) results
    type PostMutationScriptResult = Option[Either[RequestFailure, UpdateSchemaFailure]]

    // successes
    val success: PostMutationScriptResult = None
    val sum12DoesNotExist: PostMutationScriptResult = Some(Right(TargetColumnDoesNotExist(sum12.id)))

    // failures
    val unexpectedError: PostMutationScriptResult = Some(Left(UnexpectedError("test", null)))
    val failedToDiscoverDC: PostMutationScriptResult = Some(Left(FailedToDiscoverDataCoordinator))
    val dataCoordinatorBusy: PostMutationScriptResult = Some(Left(DataCoordinatorBusy))
    val doesNotExist: PostMutationScriptResult = Some(Left(DatasetDoesNotExist))

  }

  object TestCookie {

    def columnIdMap(columns: Seq[ColumnInfo[_ <: SoQLType]]) = columns.map { colInfo => (colInfo.id, colInfo.systemId) }.toMap
    def strategyMap(columns: Seq[ColumnInfo[_ <: SoQLType]]) = columns.map { colInfo => (colInfo.id, colInfo.computationStrategyInfo.get) }.toMap

    private def cookieSchema(version: Long,
                             number: Long,
                             columnIdMap: Map[UserColumnId, ColumnId],
                             strategyMap: Map[UserColumnId, ComputationStrategyInfo] = Map.empty) =
      CookieSchema(dataVersion = DataVersion(version),
                   copyNumber = CopyNumber(number),
                   primaryKey = systemPK.id,
                   columnIdMap = columnIdMap,
                   strategyMap = strategyMap,
                   obfuscationKey = "magicmagic".getBytes,
                   computationRetriesLeft = 5,
                   dataCoordinatorRetriesLeft = 5,
                   resync = false)

    // note: we are not always accounting for versions of feedback row data

    val v1Schema = cookieSchema(1, 1, columnIdMap(Seq(systemPK))) // new dataset
    val v2Schema = cookieSchema(2, 1, columnIdMap(Seq(systemPK, num1, num2))) // create num1 & num2 + row data

    val v3Schema = cookieSchema(3, 1, columnIdMap(Seq(systemPK, num1, num2, sum12)), strategyMap(Seq(sum12))) // create sum12
    val v3almost4Schema = cookieSchema(3, 1, columnIdMap(Seq(systemPK, num1, num2))) // v4 delete sum12
    val v3almost5Schema = cookieSchema(3, 1, columnIdMap(Seq(systemPK, num2))) // v5 delete num1

    private val sum23Columns = columnIdMap(Seq(systemPK, num2, num3, sum23))

    val v6Schema = cookieSchema(6, 1, sum23Columns, strategyMap(Seq(sum23))) // create num3 & sum23
    val v7Schema = cookieSchema(7, 1, sum23Columns, strategyMap(Seq(sum23))) // publish
    val v8Schema = cookieSchema(8, 2, sum23Columns, strategyMap(Seq(sum23))) // create WC
    val v9Schema = cookieSchema(9, 1, sum23Columns, strategyMap(Seq(sum23))) // discard WC

    val v10Schema = cookieSchema(10, 1, sum23Columns, strategyMap(Seq(sum23))) // update num3 rows
    val v11Schema = cookieSchema(11, 1, sum23Columns, strategyMap(Seq(sum23))) // update sum23 rows

    val v1 = Some(FeedbackCookie(v1Schema, None))
    val v2 = Some(FeedbackCookie(v2Schema, None))

    val v3 = Some(FeedbackCookie(v3Schema, None))
    val v3almost4 = Some(FeedbackCookie(v3almost4Schema, None))
    val v3almost5 = Some(FeedbackCookie(v3almost5Schema, None))

    val v6 = Some(FeedbackCookie(v6Schema, None))
    val v7 = Some(FeedbackCookie(v7Schema, None))
    val v8 = Some(FeedbackCookie(v8Schema, Some(v7Schema)))
    val v8NoPrevious = Some(FeedbackCookie(v8Schema, None))
    val v9 = Some(FeedbackCookie(v9Schema, None))
    val v10 = Some(FeedbackCookie(v10Schema, None))
    val v11 = Some(FeedbackCookie(v11Schema, None))
  }

  object TestEvent {
    import TestRows._

    def events(args: Event[SoQLType, SoQLValue]*): Iterator[Event[SoQLType, SoQLValue]] =
      (args :+ LastModifiedChanged(DateTime.now)).toIterator // events always end with a LastModifiedChanged event

    def workcopy(number: Long, version: Long) =
      WorkingCopyCreated(CopyInfo(new CopyId(number), number, LifecycleStage.Unpublished, version, DateTime.now))

    def rowdata[CV <: SoQLValue](systemPKRows: Seq[SoQLID], oldData: Seq[secondary.Row[CV]], data: Seq[secondary.Row[CV]]): RowDataUpdated[CV] = {
      RowDataUpdated(systemPKRows.zip(oldData.zip(data)).map { case (rowId, (oldRow, row)) =>
        Update(systemId = new RowId(rowId.value), data = row)(oldData = Some(oldRow))
      })
    }

    val systemPKCreated = ColumnCreated(systemPK)

    def v1Events = events(workcopy(1, 1), systemPKCreated)

    def v3Events = events(ColumnCreated(sum12))

    def v7Events = events(WorkingCopyPublished)
    def v8Events = events(workcopy(2, 8), systemPKCreated, ColumnCreated(num2), ColumnCreated(num3), ColumnCreated(sum23), DataCopied)
    def v9Events = events(WorkingCopyDropped)

    val v9Rows = toRows(systemPK, systemPKRows, Map(num2 -> num2Rows, num3 -> num3RowsV9, sum23 -> sum23RowsV9)).toSeq
    val v10Rows = toRows(systemPK, systemPKRows, Map(num2 -> num2Rows, num3 -> num3RowsV10, sum23 -> sum23RowsV9)).toSeq

    def v10Events = events(rowdata(systemPKRows, v9Rows, v10Rows))

    val v11Rows = toRows(systemPK, systemPKRows, Map(num2 -> num2Rows, num3 -> num3RowsV10, sum23 -> toSoQLNumbers(sum23RowsV11))).toSeq

    def v11Events = events(rowdata(systemPKRows, v10Rows, v9Rows))
  }

}
