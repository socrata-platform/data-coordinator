package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.truth.metadata.{DatasetMapReader, DatasetMapWriter, RollupInfo, RollupRelationship}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.soql.{BinaryTree, Compound, Leaf}
import com.socrata.soql.ast.{JoinFunc, JoinQuery, JoinTable, Select}
import com.socrata.soql.environment.{ResourceName, TableName}
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.types.SoQLType
import org.slf4j.{Logger, LoggerFactory}

object Rollup {

  private val logger = LoggerFactory.getLogger(Rollup.getClass)

  case class RollupCollocationException(ru: RollupInfo, message: String) extends Exception(message)

  def collectTableNames(selects: BinaryTree[Select]): Set[String] = {
    selects match {
      case Compound(_, l, r) =>
        collectTableNames(l) ++ collectTableNames(r)
      case Leaf(select) =>
        select.joins.foldLeft(select.from.map(_.name).filter(_ != TableName.This).toSet) { (acc, join) =>
          join.from match {
            case JoinTable(TableName(name, _)) =>
              acc + name
            case JoinQuery(selects, _) =>
              acc ++ collectTableNames(selects)
            case JoinFunc(_, _) =>
              throw new Exception("Unexpected join function")
          }
        }
    }
  }

  def parseAndCollectTableNames(soql: String): Set[String] = collectTableNames(new StandaloneParser().binaryTreeSelect(soql))

  def parseAndCollectTableNames(rollupInfo: RollupInfo): Set[String] = parseAndCollectTableNames(rollupInfo.soql)

  def updateRollupRelationships[CT](rollupInfo: RollupInfo)(implicit mapWriter:DatasetMapWriter[CT],mapReader:DatasetMapReader[CT]): Set[RollupRelationship] = {
    parseAndCollectTableNames(rollupInfo)
      .map(new ResourceName(_))
      .flatMap(a=>mapReader.datasetInfoByResourceName(a).orElse(throw RollupCollocationException(rollupInfo,s"dataset '${a.name}' could not be found")))
      .flatMap(a=>mapWriter.createRollupRelationship(rollupInfo,mapWriter.latest(a)))

  }


}
