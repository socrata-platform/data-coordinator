package com.socrata.datacoordinator.truth.sql

import scalaz.effect._
import javax.sql.DataSource
import java.sql.Connection

class DatabaseIO(val dataSource: DataSource) {
  val openConnection = IO(dataSource.getConnection())
  def closeConnection(conn: Connection): IO[Unit] = IO(conn.rollback()).ensuring(IO(conn.close()))
  def withConnection[A](f: Connection => IO[A]): IO[A] =
    openConnection.bracket(closeConnection)(f)
}
