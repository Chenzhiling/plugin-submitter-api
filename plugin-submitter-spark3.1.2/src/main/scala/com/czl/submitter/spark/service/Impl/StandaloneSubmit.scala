package com.czl.submitter.spark.service.Impl

import com.czl.submitter.spark.entity._
import com.czl.submitter.spark.enums.DriverState
import com.czl.submitter.spark.utils.{RestSparkRequestException, RestSparkResponseUtils}
import com.czl.submitter.core.consts.PluginConst
import com.czl.submitter.core.entity._
import com.czl.submitter.spark.service.SparkSubmitTrait
import com.czl.submitter.spark.service.utils.RestClientUtils
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.slf4j.{Logger, LoggerFactory}

import java.util

/**
 * Author: CHEN ZHI LING
 * Date: 2022/6/6
 * Description:
 */
object StandaloneSubmit extends SparkSubmitTrait {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  override def submit(sparkSubmitRequest: SparkSubmitRequest): SubmitResponse = {
    //build info
    val appResource: String = sparkSubmitRequest.getJarPath
    val args: util.List[String] = sparkSubmitRequest.getArgs
    val sparkVersion: String = sparkSubmitRequest.getSparkInfo.sparkVersion
    val mainClass: String = sparkSubmitRequest.getMainClass
    val properties: SparkProperties = getSparkProperties(sparkSubmitRequest)
    val restJobSubmitRequest = new RestJobSubmitRequest(appResource, args, sparkVersion, mainClass, properties)
    //build http request
    val post: HttpPost = buildSubmitHttpPost(sparkSubmitRequest)
    try {
      val mapper = new ObjectMapper()
      val message: String = mapper.writeValueAsString(restJobSubmitRequest)
      post.setEntity(new StringEntity(message))
    } catch {
      case e:JsonProcessingException =>
        logger.error("submit standalone task failed", e)
        throw new RestSparkRequestException(e)
    }
    val response: RestSparkResponse = RestSparkResponseUtils
      .executeHttpMethodAndGetResponse(RestClientUtils.getHttpClient, post, classOf[RestSparkResponse])
    logger.info("submit task success")
    new SubmitResponse(response.getSubmissionId)
  }


  override def query(sparkQueryRequest: SparkQueryRequest): QueryResponse = {
    val get: HttpGet = buildQueryHttpGet(sparkQueryRequest)
    val jobStatusResponse: RestJobStatusResponse = RestSparkResponseUtils
      .executeHttpMethodAndGetResponse(RestClientUtils.getHttpClient, get, classOf[RestJobStatusResponse])
    if (!jobStatusResponse.getSuccess) {
      logger.error("query standalone task failed")
      throw new RestSparkRequestException("submit task failed, status is {}", jobStatusResponse.getDriverState)
    }
    val state: DriverState = jobStatusResponse.getDriverState
    logger.info("query task success")
    new QueryResponse(state.toString)
  }


  override def stop(sparkKillRequest: SparkStopRequest): StopResponse = {
    val post: HttpPost = buildKillHttpPost(sparkKillRequest)
    val response: RestSparkResponse = RestSparkResponseUtils
      .executeHttpMethodAndGetResponse(RestClientUtils.getHttpClient, post, classOf[RestSparkResponse])
    logger.info("kill task success")
    new StopResponse(response.getSuccess,null)
  }


  private[this] def getSparkProperties(sparkSubmitRequest: SparkSubmitRequest): SparkProperties  = {
    val env: util.Map[String, String] = sparkSubmitRequest.getExecutorEnv
    val jars: String = sparkSubmitRequest.getJarPath
    val appName: String = sparkSubmitRequest.getAppName
    val master: String = StringUtils.replace(
      sparkSubmitRequest.getExtraParameter.get(PluginConst.SPARK_MASTER).toString,
      PluginConst.HTTP_PROTOCOL,
      PluginConst.SPARK_PROTOCOL)
    new SparkProperties(jars,appName,master,env)
  }


  /**
   * build http post request
   */
  private[this] def buildSubmitHttpPost(sparkSubmitRequest: SparkSubmitRequest): HttpPost = {
    val master: String = sparkSubmitRequest.getExtraParameter.get(PluginConst.SPARK_MASTER).toString
    //http://spark-cluster-ip:port/v1/submissions/create
    val url: String = master + PluginConst.SPARK_REST_CREATE
    val post: HttpPost = RestClientUtils.getHttpPost(
      url, null,
      RequestConfig.custom.setSocketTimeout(2000).build())
    post
  }


  private[this] def buildQueryHttpGet(sparkQueryRequest: QueryRequest): HttpGet = {
    val master: String = sparkQueryRequest.getMaster
    val submissionId: String = sparkQueryRequest.getTaskId
    val url: String = master + PluginConst.SPARK_REST_QUERY + submissionId
    val get: HttpGet = RestClientUtils.getHttpGet(
      url, null,
      RequestConfig.custom.setSocketTimeout(2000).build())
    get
  }


  private[this] def buildKillHttpPost(sparkKillRequest: StopRequest): HttpPost = {
    val master: String = sparkKillRequest.getMaster
    val submissionId: String = sparkKillRequest.getTaskId
    val url: String = master + PluginConst.SPARK_REST_KILL + submissionId
    RestClientUtils.getHttpPost(url,null,RequestConfig.custom().setSocketTimeout(2000).build())
  }
}
