package com.czl.submitter.spark.service

import com.czl.submitter.spark.entity.{SparkQueryRequest, SparkStopRequest, SparkSubmitRequest}
import com.czl.submitter.core.entity._

/**
 * Author: CHEN ZHI LING
 * Date: 2022/6/6
 * Description:
 */
trait SparkSubmitTrait {


  def submit(sparkSubmitRequest: SparkSubmitRequest): SubmitResponse


  def query(sparkQueryRequest: SparkQueryRequest): QueryResponse


  def stop(sparkKillRequest: SparkStopRequest): StopResponse
}
