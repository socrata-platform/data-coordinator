package com.socrata.datacoordinator
package secondary
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

class SqlSecondaryMap(conn: Connection) extends SecondaryMap {
  def create(storeId: String, wantsUnpublished: Boolean) =
    using(conn.prepareStatement("INSERT INTO secondary_stores (store_id, wants_unpublished) VALUES (?, ?) RETURNING system_id")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setBoolean(2, wantsUnpublished)
      using(stmt.executeQuery()) { rs =>
        val returnedSomething = rs.next()
        assert(returnedSomething, "Insert didn't return anything?")
        rs.getLong(1)
      }
    }

  def lookup(storeId: String) =
    using(conn.prepareStatement("SELECT system_id, wants_unpublished FROM secondary_stores WHERE store_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          val sId = StoreId(rs.getLong("system_id"))
          val wantsUnpublished = rs.getBoolean("wants_unpublished")
          Some((sId, wantsUnpublished))
        } else {
          None
        }
      }
    }

  def lookup(storeId: StoreId) =
    using(conn.prepareStatement("SELECT store_id, wants_unpublished FROM secondary_stores WHERE system_id = ?")) { stmt =>
      stmt.setLong(1, storeId)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          val sId = rs.getString("store_id")
          val wantsUnpublished = rs.getBoolean("wants_unpublished")
          Some((sId, wantsUnpublished))
        } else {
          None
        }
      }
    }
}
