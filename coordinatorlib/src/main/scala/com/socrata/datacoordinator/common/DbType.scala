package com.socrata.datacoordinator.common

 sealed trait DbType
 case object Redshift extends DbType
 case object Postgres extends DbType


object DbType {
  def parse: String => Option[DbType] = {
    case "redshift" => Some(Redshift)
    case "postgres" => Some(Postgres)
    case _ => None
  }
}
