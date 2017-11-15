package com.socrata.datacoordinator.common.soql.sqlreps

import java.sql.PreparedStatement

import com.socrata.soql.types.SoQLValue

trait NonLookupableRep {
  def templateForSingleLookup: String = ???

  def templateForMultiLookup(n: Int): String = ???

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = ???

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = ???

  def sql_==(literal: SoQLValue): String = ???

  def sql_in(literals: Iterable[SoQLValue]): String = ???

  def equalityIndexExpression: String = ???
}
