package com.czl.submitter.flink.service.Impl

import com.czl.submitter.flink.entity.{FlinkStopRequest, FlinkSubmitRequest, FlinkSubmitResponse}
import com.czl.submitter.core.entity.{QueryRequest, QueryResponse, StopResponse}
import com.czl.submitter.flink.FlinkInfo
import com.czl.submitter.flink.service.FlinkYarnSubmitTrait
import org.apache.flink.api.common.JobID
import org.apache.flink.client.deployment.{ClusterClientFactory, ClusterSpecification, DefaultClusterClientServiceLoader}
import org.apache.flink.client.program.{ClusterClient, PackagedProgram}
import org.apache.flink.configuration.{Configuration, DeploymentOptions}
import org.apache.flink.runtime.client.JobStatusMessage
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.yarn.configuration.{YarnConfigOptions, YarnDeploymentTarget}
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint
import org.apache.flink.yarn.{YarnClusterClientFactory, YarnClusterDescriptor}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.lang.{Boolean => JavaBool}
import scala.collection.JavaConversions._

/**
 * Author: CHEN ZHI LING
 * Date: 2022/10/27
 * Description:
 */
object YarnPerJobSubmit extends FlinkYarnSubmitTrait {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  override def setConfig(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): Unit = {
    flinkConf
      .safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.PER_JOB.getName)
      .safeSet(DeploymentOptions.ATTACHED, JavaBool.TRUE)
      .safeSet(DeploymentOptions.SHUTDOWN_IF_ATTACHED, JavaBool.TRUE)
  }


  override def doSubmit(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): FlinkSubmitResponse = {
    var clusterDescriptor: YarnClusterDescriptor = null
    var packagedProgram: PackagedProgram = null
    var clusterClient: ClusterClient[ApplicationId] = null
    val tuple: (YarnClusterDescriptor, ClusterClientFactory[ApplicationId]) = getDescriptorAndFactory(flinkConf, submitRequest.getFlinkInfo)
    clusterDescriptor = tuple._1
    try {
      val clusterSpecification: ClusterSpecification = tuple._2.getClusterSpecification(flinkConf)
      val packageProgramJobGraph: (PackagedProgram, JobGraph) = super.getJobGraph(flinkConf,submitRequest, submitRequest.supportTaskJarFile)
      packagedProgram = packageProgramJobGraph._1
      val jobGraph: JobGraph = packageProgramJobGraph._2

      clusterClient = deployInternal(
        tuple._1,
        clusterSpecification,
        submitRequest.effectiveName(),
        classOf[YarnJobClusterEntrypoint].getName,
        jobGraph,
        false
      ).getClusterClient

      val applicationId: ApplicationId = clusterClient.getClusterId
      val jobStatus: JobStatusMessage = clusterClient.listJobs().get().head
      val jobId: JobID = jobStatus.getJobId
      new FlinkSubmitResponse(applicationId.toString,jobId.toString)
    } catch {
      case exception: Exception =>
        logger.error("submit task failed", exception)
        throw exception
    } finally {
      if(null != packagedProgram) packagedProgram.close()
      if(null != clusterClient) clusterClient.close()
      if(null != clusterDescriptor) clusterDescriptor.close()
    }
  }



  override def doStop(stopRequest: FlinkStopRequest, flinkConf: Configuration): StopResponse = {
    flinkConf.safeSet(YarnConfigOptions.APPLICATION_ID, stopRequest.getMaster)

    try {
        val clusterClient: ClusterClient[ApplicationId] = {
        val clusterClientFactory = new YarnClusterClientFactory
        val applicationId: ApplicationId = clusterClientFactory.getClusterId(flinkConf)
        val clusterDescriptor: YarnClusterDescriptor = clusterClientFactory.createClusterDescriptor(flinkConf)
        clusterDescriptor.retrieve(applicationId).getClusterClient
      }
      val jobID: JobID = JobID.fromHexString(stopRequest.getTaskId)
      val savepointPath: String = cancelJob(stopRequest, jobID, clusterClient)
      val clusterClientFactory = new YarnClusterClientFactory
      val clusterDescriptor: YarnClusterDescriptor = clusterClientFactory.createClusterDescriptor(flinkConf)
      clusterDescriptor.killCluster(ApplicationId.fromString(stopRequest.getMaster))
      new StopResponse(true, savepointPath)
    } catch {
      case exception: Exception =>
        logger.error("stop task failed", exception)
        throw exception
    }
  }


  private[this] def getDescriptorAndFactory(flinkConf: Configuration,flinkVersion: FlinkInfo):
  (YarnClusterDescriptor,ClusterClientFactory[ApplicationId]) = {
    val flinkHome: String = flinkVersion.flinkHome
    val clusterClientServiceLoader = new DefaultClusterClientServiceLoader
    val clientFactory: ClusterClientFactory[ApplicationId] =
      clusterClientServiceLoader.getClusterClientFactory[ApplicationId](flinkConf)

    val yarnClusterDescriptor: YarnClusterDescriptor = {
      clientFactory.createClusterDescriptor(flinkConf).asInstanceOf[YarnClusterDescriptor]
    }
    //flink dist-jar
    yarnClusterDescriptor.setLocalJarPath(new Path(flinkVersion.flinkDistJar.getPath))
    yarnClusterDescriptor.addShipFiles(List(new File(s"$flinkHome/lib")))
    (yarnClusterDescriptor,clientFactory)
  }

  override def query(queryRequest: QueryRequest): QueryResponse = {
    YarnSessionSubmit.query(queryRequest)
  }
}
