package com.socrata.datacoordinator.truth.sql

import java.sql.PreparedStatement

trait SqlPKableColumnRep[Type, Value] extends SqlColumnRep[Type, Value] {
  /** Generates sql equivalent to "column in (?, ...)" where there are `n` placeholders to be filled in by
    * `prepareMultiLookup`.
    * @param n The number of values to prepare slots for.
    * @note `n` must be greater than zero.
    */
  def templateForMultiLookup(n: Int): String

  /** Fill in one placeholder in a template created by `prepareMultiLookup`.
    * @return The position of the first prepared statement parameter after this placeholder.
    */
  def prepareMultiLookup(stmt: PreparedStatement, v: Value, start: Int): Int

  /** Generates a SQL expression equivalent to "`column in (literals...)`".
    * @param literals The `StringBuilder` to which to add the data.  Must be non-empty.
    *                 The individual values' types must be equal to (not merely compatible with!)
    *                 `representedType`.
    * @return An expression suitable for splicing into a SQL statement.
    */
  def sql_in(literals: Iterable[Value]): String

  /** Generates SQL equivalent to "column = ?", where the placeholder can be filled
    * in by `prepareSingleLookup`
    */
  def templateForSingleLookup: String

  /** Fill in the placeholder in a template created by `prepareSingleLookup`.
    * @return The position of the first prepared statement parameter after this placeholder.
    */
  def prepareSingleLookup(stmt: PreparedStatement, v: Value, start: Int): Int

  /** Generates a SQL expression equivalent to "`column = literal`".
    * @param literal The `StringBuilder` to which to add the data.  The value's type
    *                must be equal to (not merely compatible with!) `representedType`.
    * @return An expression suitable for splicing into a SQL statement.
    */
  def sql_==(literal: Value): String

  /** Generates a SQL expression which can be used to create an index on this column suitable
    * for use in equality expressions. */
  def equalityIndexExpression: String
}
