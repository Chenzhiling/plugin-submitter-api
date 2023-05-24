package com.czl.submitter.presto.util

import com.czl.submitter.presto.entity.SqlQueryResponse
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hc.client5.http.fluent.Request
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.util.Timeout
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/23
 * Description:
 */
object PrestoRestUtils {


  @transient
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats


  def sqlQueryTask(url: String, user: String, sql: String): SqlTaskResponse = {
    val result: String = Request.post(url)
      .connectTimeout(Timeout.ofSeconds(10))
      .responseTimeout(Timeout.ofSeconds(10))
      .addHeader("content-type", "application/json")
      .addHeader("X-Presto-User", user)
      .body(new StringEntity(sql))
      .execute()
      .returnContent()
      .asString(StandardCharsets.UTF_8)
    getResponse(result)
  }


  @tailrec
  def subRequest(prestoSubmitResponse: SqlTaskResponse, data: ListBuffer[List[AnyRef]]): SqlQueryResponse = {
      val subResult: String = Request.get(prestoSubmitResponse.nextUrl)
        .connectTimeout(Timeout.ofSeconds(10))
        .responseTimeout(Timeout.ofSeconds(10))
        .execute()
        .returnContent()
        .asString(StandardCharsets.UTF_8)
      val subResponse: SqlTaskResponse = getResponse(subResult)
      if (CollectionUtils.isNotEmpty(subResponse.data.asJava)) {
        val subData: List[List[AnyRef]] = subResponse.data
        subData.foreach((x: List[AnyRef]) => data.append(x))
      }
      if ("FINISHED".equals(subResponse.state) && StringUtils.isEmpty(subResponse.nextUrl)) {
         return SqlQueryResponse(data.toList.asJava)
      }
      subRequest(subResponse, data)
  }

  private def getResponse(result: String): SqlTaskResponse = {
    val response: SqlTaskResponse = Try(parse(result)) match {
      case Success(ok) =>
        SqlTaskResponse(
          (ok \ "nextUri").extractOpt[String].orNull,
          (ok \ "stats").extractOpt[Map[String, AnyRef]].orNull.get("state").mkString,
          (ok \ "data").extractOpt[List[List[AnyRef]]].orNull,
        )
      case Failure(_) => null
    }
    response
  }
}


private[presto] case class SqlTaskResponse(nextUrl: String,
                                           state: String,
                                           data: List[List[AnyRef]]) {
}
