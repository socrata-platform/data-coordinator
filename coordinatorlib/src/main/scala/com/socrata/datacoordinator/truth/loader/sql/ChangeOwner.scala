package com.socrata.datacoordinator.truth.loader.sql

import java.sql.Connection

/**
 * To facilitate password rotation, we alternate between 2 users.
 * These two users must inherit rights from the same user and share the same alpha name with numeric suffix.
 * i.e. blist => blist1, blist2
 */
object ChangeOwner {

  def sql(conn: Connection, tableName: String): String = {
    "ALTER TABLE \"%s\" OWNER TO %s;".format(tableName, canonicalUser(conn))
  }

  def canonicalUser(conn: Connection): String = {
    val user = conn.getMetaData.getUserName
    user.replaceFirst("(\\D+)(\\d*)", "$1")
  }
}
