package com.czl.submitter.presto.entity

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/23
 * Description:
 */
case class SqlQueryRequest(master:String,
                           sql: String,
                           user: String)
