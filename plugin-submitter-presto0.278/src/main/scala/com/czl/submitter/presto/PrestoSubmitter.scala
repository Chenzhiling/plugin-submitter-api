package com.czl.submitter.presto

import com.czl.submitter.presto.entity.{ClusterInfo, NodeInfo, SqlQueryRequest, SqlQueryResponse, StatusQueryRequest, StatusQueryResponse}
import com.czl.submitter.presto.service.impl.StandaloneSubmit

import scala.collection.JavaConverters._
/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/23
 * Description:
 */
object PrestoSubmitter {


  def sqlQuery(sqlQueryRequest: SqlQueryRequest): SqlQueryResponse = {
    StandaloneSubmit.sqlQuery(sqlQueryRequest)
  }


  def statusQueryById(statusQuery: StatusQueryRequest): StatusQueryResponse = {
    StandaloneSubmit.statusQueryById(statusQuery)
  }


  def statusQuery(master: String): java.util.List[StatusQueryResponse] = {
    StandaloneSubmit.statusQuery(master).asJava
  }


  def nodeInfoQuery(master: String): NodeInfo = {
    StandaloneSubmit.nodeInfoQuery(master)
  }


  def clusterInfoQuery(master: String): ClusterInfo = {
    StandaloneSubmit.clusterInfoQuery(master)
  }
}
