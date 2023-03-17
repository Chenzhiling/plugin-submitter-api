package com.czl.submitter.flink.service.Impl

import com.czl.submitter.flink.entity.{FlinkStopRequest, FlinkSubmitRequest, FlinkSubmitResponse}
import com.czl.submitter.core.consts.PluginConst
import com.czl.submitter.core.entity.{QueryRequest, QueryResponse, StopResponse}
import com.czl.submitter.flink.service.FlinkSubmitTrait
import org.apache.flink.api.common.JobID
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import org.apache.flink.client.program.{ClusterClient, PackagedProgram}
import org.apache.flink.configuration.{Configuration, DeploymentOptions}
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.yarn.YarnClusterDescriptor
import org.apache.flink.yarn.configuration.{YarnConfigOptions, YarnDeploymentTarget}
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/6/10
 * Description:
 */
object YarnSessionSubmit extends FlinkSubmitTrait {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  override def setConfig(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): Unit = {
    flinkConf
      //set yarn-session id
      .safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
      .safeSet(YarnConfigOptions.APPLICATION_ID, submitRequest.getExtraParameter.get(PluginConst.KEY_YARN_ID).toString)
  }



  override def doSubmit(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): FlinkSubmitResponse = {
    var clusterDescriptor: YarnClusterDescriptor = null
    var packageProgram: PackagedProgram = null
    var client: ClusterClient[ApplicationId] = null
    try {

      val yarnClusterDescriptor: (ApplicationId, YarnClusterDescriptor) = getYarnSessionClusterDescriptor(flinkConf)
      val yarnClusterId: ApplicationId = yarnClusterDescriptor._1
      clusterDescriptor = yarnClusterDescriptor._2

      val packageProgramJobGraph: (PackagedProgram, JobGraph) = {
        super.getJobGraph(flinkConf, submitRequest, submitRequest.supportTaskJarFile)
      }
      packageProgram = packageProgramJobGraph._1
      val jobGraph: JobGraph = packageProgramJobGraph._2

      client = clusterDescriptor.retrieve(yarnClusterId).getClusterClient
      val jobId: String = client.submitJob(jobGraph).get().toString
      new FlinkSubmitResponse(yarnClusterId.toString, jobId)
    } catch {
      case exception: Exception =>
        logger.error("submit task failed", exception)
        throw exception
    } finally {
      if(null != packageProgram) packageProgram.close()
      if(null != client) client.close()
      if(null != clusterDescriptor) clusterDescriptor.close()
    }
  }


  override def doStop(stopRequest: FlinkStopRequest, flinkConf: Configuration): StopResponse = {
    flinkConf.safeSet(YarnConfigOptions.APPLICATION_ID, stopRequest.getExtraParameter.get(PluginConst.KEY_YARN_ID).toString)
    flinkConf.safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
    var clusterDescriptor: YarnClusterDescriptor = null
    var client: ClusterClient[ApplicationId] = null
    try {
      val yarnClusterDescriptor: (ApplicationId, YarnClusterDescriptor) = getYarnSessionClusterDescriptor(flinkConf)
      clusterDescriptor = yarnClusterDescriptor._2
      client = clusterDescriptor.retrieve(yarnClusterDescriptor._1).getClusterClient
      val jobID: JobID = JobID.fromHexString(stopRequest.getTaskId)
      val actionResult: String = cancelJob(stopRequest, jobID, client)
      new StopResponse(true, actionResult)
    } catch {
      case exception: Exception=>
        logger.error("stop task failed", exception)
        throw exception
    } finally {
      if(null != client) client.close()
      if(null != clusterDescriptor) clusterDescriptor.close()
    }
  }


  override def query(queryRequest: QueryRequest): QueryResponse = {
    this.doQuery(queryRequest)
  }


  private[this] def getYarnSessionClusterDescriptor(flinkConfig: Configuration): (ApplicationId, YarnClusterDescriptor) = {
    val serviceLoader = new DefaultClusterClientServiceLoader
    val clientFactory: ClusterClientFactory[ApplicationId] = serviceLoader.getClusterClientFactory[ApplicationId](flinkConfig)
    val yarnClusterId: ApplicationId = clientFactory.getClusterId(flinkConfig)
    require(yarnClusterId != null)
    val clusterDescriptor: YarnClusterDescriptor =
      clientFactory.createClusterDescriptor(flinkConfig).asInstanceOf[YarnClusterDescriptor]
    (yarnClusterId, clusterDescriptor)
  }
}
