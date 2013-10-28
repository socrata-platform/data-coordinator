package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.RowId

sealed abstract class Operation[+CV]
case class Insert[CV](systemId: RowId, data: Row[CV]) extends Operation[CV]
case class Update[CV](systemId: RowId, data: Row[CV])(val oldData: Option[Row[CV]]) extends Operation[CV]
case class Delete[CV](systemId: RowId)(val oldData: Option[Row[CV]]) extends Operation[CV]
