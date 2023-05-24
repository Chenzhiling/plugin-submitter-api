package com.czl.submitter.presto.entity

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/24
 * Description:
 */
case class StatusQueryResponse(state: String,
                               query: String,
                               queryType: String,
                               user: String,
                               elapsedTime: String) {

}
