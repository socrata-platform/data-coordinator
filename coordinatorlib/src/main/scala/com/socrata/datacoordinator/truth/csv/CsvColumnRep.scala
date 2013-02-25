package com.socrata.datacoordinator.truth.csv

trait CsvColumnReadRep[CT, CV] {
  val size: Int
  val representedType: CT
  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]): Option[CV]
}

trait CsvColumnRep[CT, CV] extends CsvColumnReadRep[CT, CV]
