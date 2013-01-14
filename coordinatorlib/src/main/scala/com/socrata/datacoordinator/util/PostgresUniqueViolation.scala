package com.socrata.datacoordinator.util

import org.postgresql.util.PSQLException

sealed trait PostgresUniqueViolation

object PostgresUniqueViolation {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresUniqueViolation])

  private val ExpectedDetail = """Key \(([^=]*)\)=\(.*\) already exists.""".r

  def unapplySeq(ex: PSQLException): Option[Seq[String]] = {
    if(ex.getSQLState == "23505") {
      ex.getServerErrorMessage.getDetail match {
        case ExpectedDetail(key) =>
          Some(key.split(", "))
        case msg =>
          log.error("Unable to extract useful information from unique constraint violation detail: {}", msg)
          Some(Nil)
      }
    } else {
      None
    }
  }
}
