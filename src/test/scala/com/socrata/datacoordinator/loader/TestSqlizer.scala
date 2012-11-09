package com.socrata.datacoordinator.loader

class TestSqlizer extends Sqlizer {
  var transactionsLogged = 0
  var tablesLocked = 0

  def logTransactionComplete() {
    transactionsLogged += 1
  }

  def lockTableAgainstWrites(table: String) = {
    tablesLocked += 1
    "SELECT 0"
  }
}
