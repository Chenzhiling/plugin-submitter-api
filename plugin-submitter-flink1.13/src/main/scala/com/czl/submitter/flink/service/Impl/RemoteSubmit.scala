package com.czl.submitter.flink.service.Impl

import com.czl.submiiter.flink.entity.{FlinkStopRequest, FlinkSubmitRequest, FlinkSubmitResponse}
import com.czl.submitter.core.entity.{QueryRequest, QueryResponse, StopResponse}
import com.czl.submitter.flink.service.FlinkSubmitTrait
import com.czl.submitter.flink.service.tool.FlinkSubmitUtils
import org.apache.flink.api.common.JobID
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader, StandaloneClusterDescriptor, StandaloneClusterId}
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.{Configuration, DeploymentOptions, RestOptions}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.lang.{Integer => JavaInt}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/27
 * Description:
 */
object RemoteSubmit extends FlinkSubmitTrait {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  override def doSubmit(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): FlinkSubmitResponse = {
    restfulApiSubmit(flinkConf,submitRequest.supportTaskJarFile)
  }


  override def setConfig(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): Unit = {
    flinkConf
      .safeSet(RestOptions.ADDRESS, submitRequest.getExtraParameter.get(RestOptions.ADDRESS.key()).toString)
      .safeSet[JavaInt](RestOptions.PORT, submitRequest.getExtraParameter.get(RestOptions.PORT.key()).toString.toInt)
  }


  override def doStop(stopRequest: FlinkStopRequest, flinkConf: Configuration): StopResponse = {
    flinkConf
      .safeSet(DeploymentOptions.TARGET, stopRequest.getExecutionMode.getName)
      .safeSet(RestOptions.ADDRESS, stopRequest.getExtraParameter.get(RestOptions.ADDRESS.key()).toString)
      .safeSet[JavaInt](RestOptions.PORT, stopRequest.getExtraParameter.get(RestOptions.PORT.key()).toString.toInt)
    val standAloneDescriptor: (StandaloneClusterId, StandaloneClusterDescriptor) =
      getStandAloneClusterDescriptor(flinkConf)
    var client: ClusterClient[StandaloneClusterId] = null
    try {
      client = standAloneDescriptor._2.retrieve(standAloneDescriptor._1).getClusterClient
      val jobID: JobID = JobID.fromHexString(stopRequest.getTaskId)
      val actionResult: String = cancelJob(stopRequest, jobID, client)
      new StopResponse(true, actionResult)
    } catch {
      case e: Exception =>
        logger.error("stop task failed",e)
        throw e
    } finally {
      if (client != null) client.close()
      if (standAloneDescriptor != null) standAloneDescriptor._2.close()
    }
  }


  override def query(queryRequest: QueryRequest): QueryResponse = {
    this.doQuery(queryRequest)
  }


  def restfulApiSubmit(flinkConfig: Configuration, fatJar: File): FlinkSubmitResponse = {
    var clusterDescriptor: StandaloneClusterDescriptor = null
    var client: ClusterClient[StandaloneClusterId] = null
    try {
      val standAloneDescriptor: (StandaloneClusterId, StandaloneClusterDescriptor) = getStandAloneClusterDescriptor(flinkConfig)
      val yarnClusterId: StandaloneClusterId = standAloneDescriptor._1
      clusterDescriptor = standAloneDescriptor._2

      client = clusterDescriptor.retrieve(yarnClusterId).getClusterClient
      val jobId: String = FlinkSubmitUtils.submitJarViaRestApi(client.getWebInterfaceURL, fatJar, flinkConfig)
      new FlinkSubmitResponse(client.getClusterId.toString, jobId)
    } catch {
      case e: Exception =>
        logger.error("submit task by restful failed", e)
        throw e
    }
  }


  def getStandAloneClusterDescriptor(flinkConfig: Configuration): (StandaloneClusterId, StandaloneClusterDescriptor) = {
    val serviceLoader = new DefaultClusterClientServiceLoader
    val clientFactory: ClusterClientFactory[Nothing] = serviceLoader.getClusterClientFactory(flinkConfig)
    val standaloneClusterId: StandaloneClusterId = clientFactory.getClusterId(flinkConfig)
    val standaloneClusterDescriptor: StandaloneClusterDescriptor =
      clientFactory.createClusterDescriptor(flinkConfig).asInstanceOf[StandaloneClusterDescriptor]
    (standaloneClusterId, standaloneClusterDescriptor)
  }
}
