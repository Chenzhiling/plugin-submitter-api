package com.czl.submitter.presto.service

import com.czl.submitter.presto.entity.{SqlQueryRequest, SqlQueryResponse}

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/23
 * Description:
 */
trait PrestoTrait {


  def sqlQuery(sqlQueryRequest: SqlQueryRequest): SqlQueryResponse
}
