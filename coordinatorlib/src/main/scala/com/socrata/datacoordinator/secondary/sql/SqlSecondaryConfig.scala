package com.socrata.datacoordinator.secondary
package sql

import java.sql.{Timestamp, Connection}

import org.joda.time.DateTime
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.TimingReport

class SqlSecondaryConfig(conn: Connection, timingReport: TimingReport) extends SecondaryConfig {
  private def t = timingReport

  def lookup(storeId: String): Option[SecondaryConfigInfo] =
    using(conn.prepareStatement("SELECT store_id, next_run_time, interval_in_seconds FROM secondary_stores_config WHERE store_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      using(t("lookup-store-config", "store_id" -> storeId)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(SecondaryConfigInfo(rs.getString("store_id"), new DateTime(rs.getTimestamp("next_run_time").getTime), rs.getInt("interval_in_seconds")))
        } else {
          None
        }
      }
    }

  def create(secondaryInfo: SecondaryConfigInfo): SecondaryConfigInfo =
    using(conn.prepareStatement("INSERT INTO secondary_stores_config (store_id, next_run_time, interval_in_seconds) VALUES (?, ?, ?)")) { stmt =>
      stmt.setString(1, secondaryInfo.storeId)
      stmt.setTimestamp(2, new Timestamp(secondaryInfo.nextRunTime.getMillis))
      stmt.setInt(3, secondaryInfo.runIntervalSeconds)
      t("create-store-config", "store_id" -> secondaryInfo.storeId)(stmt.execute())
      secondaryInfo
    }

  def updateNextRunTime(storeId: String, newNextRunTime: DateTime) {
    using(conn.prepareStatement("UPDATE secondary_stores_config SET next_run_time = ? WHERE store_id = ?")) { stmt =>
      stmt.setTimestamp(1, new Timestamp(newNextRunTime.getMillis))
      stmt.setString(2, storeId)
      t("update-next-runtime", "store-id" -> storeId)(stmt.execute())
    }
  }
}
