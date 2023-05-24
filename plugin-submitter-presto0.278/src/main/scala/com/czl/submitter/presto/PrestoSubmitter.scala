package com.czl.submitter.presto

import com.czl.submitter.presto.entity.{SqlQueryRequest, SqlQueryResponse, StatusQueryRequest, StatusQueryResponse}
import com.czl.submitter.presto.service.impl.StandaloneSubmit

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/23
 * Description:
 */
object PrestoSubmitter {


  def sqlQuery(sqlQueryRequest: SqlQueryRequest): SqlQueryResponse = {
    StandaloneSubmit.sqlQuery(sqlQueryRequest)
  }

  def statusQuery(statusQuery: StatusQueryRequest): StatusQueryResponse = {
    StandaloneSubmit.statusQuery(statusQuery)
  }
}
