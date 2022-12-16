package com.socrata.datacoordinator.common.util

import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.truth.metadata.RollupDatasetRelation
import com.socrata.soql.environment.ResourceName

import java.sql.ResultSet

object SqlExtractor {

  def extractSeqRollupDatasetRelation(rs:ResultSet):Set[RollupDatasetRelation]={
    Iterator.continually(rs)
      .takeWhile(_.next())
      .map(rs =>
        RollupDatasetRelation(ResourceName(rs.getString("primary_dataset")), new RollupName(rs.getString("name")), rs.getString("soql"), rs.getArray("secondary_datasets").getArray.asInstanceOf[Array[String]].map(ResourceName(_)).toSet)
      ).toList.toSet
  }

}
