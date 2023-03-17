package com.czl.submitter.spark.service.utils

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicNameValuePair
import org.apache.http.protocol.HTTP
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, NameValuePair}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/16
 * Description:
 */
object RestClientUtils {


  private[this] lazy val TYPE_JSON = "application/json"


  private[this] lazy val CHARSET_UTF_8 = "charset=UTF-8"


  private[this] lazy val HTTP_HEADER: String = TYPE_JSON + ";" + CHARSET_UTF_8


  private[this] lazy val connectionManager: PoolingHttpClientConnectionManager = {
    val connectionManager = new PoolingHttpClientConnectionManager
    connectionManager.setMaxTotal(50)
    connectionManager.setDefaultMaxPerRoute(5)
    connectionManager
  }


  def httpGetRequest(url: String, config: RequestConfig): String = {
    getResult(getHttpGet(url, null, config))
  }


  def httpPostRequest(url: String, config: RequestConfig): String = {
    getResult(getHttpPost(url, null, config))
  }


  private[this] def getResult(request: HttpRequestBase): String = {
    val httpClient: CloseableHttpClient = getHttpClient
    try {
      val response: CloseableHttpResponse = httpClient.execute(request)
      val entity: HttpEntity = response.getEntity
      if (entity != null) {
        val result: String = EntityUtils.toString(entity)
        response.close()
        result
      } else null
    } catch {
      case e: Exception => throw e
    } finally {
      request.releaseConnection()
    }
  }


  def getHttpClient: CloseableHttpClient =
    HttpClients.custom.setConnectionManager(connectionManager).build


  def getHttpGet(url: String,
                 params: Map[String, AnyRef] = null,
                 config: RequestConfig = null): HttpGet = {
    val httpGet: HttpGet = params match {
      case null => new HttpGet(url)
      case _ =>
        val ub: URIBuilder = uriBuilder(url, params)
        new HttpGet(ub.build)
    }
    if (null != config) {
      httpGet.setConfig(config)
    }
    httpGet
  }


  def getHttpPost(url: String,
                  params: Map[String, AnyRef] = null,
                  config: RequestConfig = null): HttpPost = {
    val httpPost: HttpPost = params match {
      case null => new HttpPost(url)
      case _ =>
        val ub: URIBuilder = uriBuilder(url, params)
        new HttpPost(ub.build)
    }
    if (null != config) {
      httpPost.setConfig(config)
    }
    httpPost.setHeader(HTTP.CONTENT_TYPE, HTTP_HEADER)
    httpPost
  }


  private[this] def uriBuilder(url: String, params: Map[String, AnyRef]): URIBuilder = {
    val uriBuilder = new URIBuilder
    uriBuilder.setPath(url)
    uriBuilder.setParameters(params2NVPS(params))
    uriBuilder
  }


  private[this] def params2NVPS(params: Map[String, AnyRef]): List[NameValuePair] = {
    val pairs = new ListBuffer[NameValuePair]
    for (param <- params.entrySet) {
      val keyPair = new BasicNameValuePair(param.getKey, String.valueOf(param.getValue))
      pairs.add(keyPair)
    }
    pairs.toList
  }
}
