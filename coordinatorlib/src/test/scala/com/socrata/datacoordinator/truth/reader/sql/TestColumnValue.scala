package com.socrata.datacoordinator.truth.reader.sql

import com.socrata.datacoordinator.id.{RowVersion, RowId}

sealed abstract class TestColumnValue
case class IdValue(value: RowId) extends TestColumnValue
case class VersionValue(value: RowVersion) extends TestColumnValue
case class NumberValue(value: Long) extends TestColumnValue
case class StringValue(value: String) extends TestColumnValue
case object NullValue extends TestColumnValue
