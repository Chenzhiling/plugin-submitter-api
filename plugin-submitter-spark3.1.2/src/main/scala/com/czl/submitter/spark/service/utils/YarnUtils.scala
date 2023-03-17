package com.czl.submitter.spark.service.utils

import org.apache.hadoop.service.Service.STATE
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import java.io.File
import java.util

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/17
 * Description:
 */
object YarnUtils {


  var reusableYarnClient: YarnClient = _


  private[this] lazy val YARN_RM_HOSTNAME: String = "yarn.resourcemanager.hostname."


  private[this] lazy val YARN_RM_ADDRESS: String = "yarn.resourcemanager.address."


  def yarnClient(yarnFile: String): YarnClient = {
    if (reusableYarnClient == null || !reusableYarnClient.isInState(STATE.STARTED)) {
      reusableYarnClient = YarnClient.createYarnClient
      val yarnConf: YarnConfiguration = getYarnConf(yarnFile)
      reusableYarnClient.init(yarnConf)
      reusableYarnClient.start()
    }
    reusableYarnClient
  }


  def getState(appId: String, yarnFile:String): YarnApplicationState = {
    yarnClient(yarnFile)
    val id: ApplicationId = ApplicationId.fromString(appId)
    try {
      val report: ApplicationReport = reusableYarnClient.getApplicationReport(id)
      report.getYarnApplicationState
    } catch {
      case _: Exception => null
    }
  }


  def getFinalStatue(appId: String, yarnFile:String): FinalApplicationStatus = {
    yarnClient(yarnFile)
    val id: ApplicationId = ApplicationId.fromString(appId)
    try {
      val report: ApplicationReport = reusableYarnClient.getApplicationReport(id)
      report.getFinalApplicationStatus
    } catch {
      case _: Exception => null
    }
  }


  def getYarnConf(yarnFile: String): YarnConfiguration = {
    val configuration = new YarnConfiguration()
    val file = new File(yarnFile)
    if (file.exists()) {
      configuration.addResource(file.toURI.toURL)
    }
    haYarnConf(configuration)
    configuration
  }


  def killYarn(appId: String, yarnFile:String): Boolean = {
    yarnClient(yarnFile)
    try {
      reusableYarnClient.killApplication(ApplicationId.fromString(appId))
      true
    } catch {
      case _: Exception => false
    }
  }


  /**
   * support ha
   */
  private[this] def haYarnConf(yarnConfiguration: YarnConfiguration): YarnConfiguration = {
    val iterator: util.Iterator[util.Map.Entry[String, String]] = yarnConfiguration.iterator()
    while (iterator.hasNext) {
      val entry: util.Map.Entry[String, String] = iterator.next()
      val key: String = entry.getKey
      val value: String = entry.getValue
      if (key.startsWith(YARN_RM_HOSTNAME)) {
        val rm: String = key.substring(YARN_RM_HOSTNAME.length)
        val addressKey: String = YARN_RM_ADDRESS + rm
        if (yarnConfiguration.get(addressKey) == null) {
          yarnConfiguration.set(addressKey, value + ":" + YarnConfiguration.DEFAULT_RM_PORT)
        }
      }
    }
    yarnConfiguration
  }
}