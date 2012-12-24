package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.sql.{SqlPKableColumnRep, SqlColumnRep}

abstract class RepBasedSchemaLoader[CT, CV](conn: Connection, dataTableName: String) extends SchemaLoader[CT, CV] {
  def repFor(baseName: String, typ: CT): SqlColumnRep[CT, CV]

  // overriddeden when things need to go in tablespaces
  def postgresTablespaceSuffix: String = ""

  def create() {
    using(conn.createStatement()) { stmt =>
      stmt.execute("CREATE TABLE " + dataTableName + " ()" + postgresTablespaceSuffix)
    }
  }

  def addColumn(baseName: String, typ: CT) {
    val rep = repFor(baseName, typ)
    using(conn.createStatement()) { stmt =>
      for((col, colTyp) <- rep.physColumns.zip(rep.sqlTypes)) {
        stmt.execute("ALTER TABLE " + dataTableName + " ADD COLUMN " + col + " " + colTyp + " NULL")
      }
    }
  }

  def dropColumn(baseName: String, typ: CT) {
    val rep = repFor(baseName, typ)
    using(conn.createStatement()) { stmt =>
      for(col <- rep.physColumns) {
        stmt.execute("ALTER TABLE " + dataTableName + " DROP COLUMN " + col)
      }
    }
  }

  def makePrimaryKey(baseName: String, typ: CT): Boolean = {
    repFor(baseName, typ) match {
      case rep: SqlPKableColumnRep[CT, CV] => // FIXME I think scala 2.10 will make this warning go away
        using(conn.createStatement()) { stmt =>
          stmt.execute("CREATE INDEX " + dataTableName + "_" + baseName + " ON " + dataTableName + "(" + rep.equalityIndexExpression + ")" + postgresTablespaceSuffix)
          for(col <- rep.physColumns) {
            stmt.execute("ALTER TABLE " + dataTableName + " ALTER " + col + " SET NOT NULL")
          }
        }
        true
      case _ =>
        false
    }
  }

  def dropPrimaryKey(baseName: String, typ: CT): Boolean = {
    repFor(baseName, typ) match {
      case rep: SqlPKableColumnRep[CT, CV] => // FIXME I think scala 2.10 will make this warning go away
        using(conn.createStatement()) { stmt =>
          stmt.execute("DROP INDEX " + dataTableName + "_" + baseName)
          for(col <- rep.physColumns) {
            stmt.execute("ALTER TABLE " + dataTableName + " ALTER " + col + " DROP NOT NULL")
          }
        }
        true
      case _ =>
        false
    }
  }
}
