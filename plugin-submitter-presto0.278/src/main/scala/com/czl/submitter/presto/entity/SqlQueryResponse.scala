package com.czl.submitter.presto.entity

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/23
 * Description:
 */
case class SqlQueryResponse(schema: java.util.List[String],
                            data: java.util.List[List[AnyRef]])
