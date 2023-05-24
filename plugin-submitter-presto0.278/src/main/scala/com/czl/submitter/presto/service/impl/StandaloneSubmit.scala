package com.czl.submitter.presto.service.impl

import com.czl.submitter.presto.entity.{SqlQueryRequest, SqlQueryResponse, StatusQueryRequest, StatusQueryResponse}
import com.czl.submitter.presto.service.PrestoTrait
import com.czl.submitter.presto.util.{PrestoRestUtils, SqlTaskResponse}

import scala.collection.mutable.ListBuffer

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/23
 * Description:
 */
object StandaloneSubmit extends PrestoTrait {


  override def sqlQuery(submitRequest: SqlQueryRequest): SqlQueryResponse = {
    val master: String = submitRequest.master
    val sql: String = submitRequest.sql
    val user: String = submitRequest.user
    val url: String = s"$master/v1/statement"
    val response: SqlTaskResponse = PrestoRestUtils.sqlQueryTask(url, user, sql)
    PrestoRestUtils.subRequest(response, new ListBuffer[List[AnyRef]])
  }

  override def statusQuery(statusQuery: StatusQueryRequest): StatusQueryResponse = {
    val url = s"${statusQuery.master}/v1/query/${statusQuery.taskId}"
    PrestoRestUtils.statusQuery(url)
  }
}
