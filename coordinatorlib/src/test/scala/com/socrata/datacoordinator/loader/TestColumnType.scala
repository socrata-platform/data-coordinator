package com.socrata.datacoordinator.loader

sealed abstract class TestColumnType {
  def isCompatible(v: TestColumnValue): Boolean
}

case object LongColumn extends TestColumnType {
  def isCompatible(v: TestColumnValue) = v match {
    case NullValue | LongValue(_) => true
    case _ => false
  }
}

case object StringColumn extends TestColumnType {
  def isCompatible(v: TestColumnValue) = v match {
    case NullValue | StringValue(_) => true
    case _ => false
  }
}
