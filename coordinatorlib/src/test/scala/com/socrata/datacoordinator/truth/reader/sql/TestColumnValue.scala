package com.socrata.datacoordinator.truth.reader.sql

sealed abstract class TestColumnValue
case class IdValue(value: Long) extends TestColumnValue
case class NumberValue(value: Long) extends TestColumnValue
case class StringValue(value: String) extends TestColumnValue
case object NullValue extends TestColumnValue
