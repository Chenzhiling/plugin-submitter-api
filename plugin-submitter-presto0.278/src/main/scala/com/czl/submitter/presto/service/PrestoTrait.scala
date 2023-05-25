package com.czl.submitter.presto.service

import com.czl.submitter.presto.entity.{SqlQueryRequest, SqlQueryResponse, StatusQueryRequest, StatusQueryResponse}

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/23
 * Description:
 */
trait PrestoTrait {


  def sqlQuery(sqlQueryRequest: SqlQueryRequest): SqlQueryResponse


  def statusQueryById(statusQuery: StatusQueryRequest): StatusQueryResponse


  def statusQuery(master: String): List[StatusQueryResponse]
}
