package com.socrata.datacoordinator.truth.reader.sql

sealed abstract class TestColumnType
case object IdType extends TestColumnType
case object NumberType extends TestColumnType
case object StringType extends TestColumnType
