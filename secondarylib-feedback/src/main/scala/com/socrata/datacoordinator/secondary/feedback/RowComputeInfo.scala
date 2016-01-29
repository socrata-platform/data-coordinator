package com.socrata.datacoordinator.secondary.feedback

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.secondary

trait RowComputeInfo[CV] {

  def data: secondary.Row[CV]
  def targetColId: UserColumnId

}
