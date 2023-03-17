package com.czl.submitter.spark.service.Impl

import com.czl.submitter.spark.entity.{SparkQueryRequest, SparkStopRequest, SparkSubmitRequest}
import com.czl.submitter.spark.enums.DriverState
import com.czl.submitter.core.consts.PluginConst
import com.czl.submitter.core.entity._
import com.czl.submitter.spark.service.SparkSubmitTrait
import com.czl.submitter.spark.service.utils.YarnUtils
import org.apache.hadoop.yarn.api.records.{FinalApplicationStatus, YarnApplicationState}
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.concurrent.CountDownLatch
import scala.collection.JavaConversions._

/**
 * Author: CHEN ZHI LING
 * Date: 2022/11/7
 * Description:
 */
object YarnSubmit extends SparkSubmitTrait {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  override def submit(sparkSubmitRequest: SparkSubmitRequest): SubmitResponse = {
    val sparkLauncher: SparkLauncher = setConfig(sparkSubmitRequest)
    val countDownLatch = new CountDownLatch(1)
    val handle: SparkAppHandle = sparkLauncher.setVerbose(true).startApplication( new SparkAppHandle.Listener {
      override def stateChanged(handle: SparkAppHandle): Unit = {
        if (handle.getState.equals(SparkAppHandle.State.SUBMITTED)){
            countDownLatch.countDown()
        }
      }
      override def infoChanged(handle: SparkAppHandle): Unit = logger.info("state Changed,submit {} success", handle.getAppId)
    })
    countDownLatch.await()
    logger.info("submit {} task success", sparkSubmitRequest.effectiveName())
    new SubmitResponse(handle.getAppId)
  }


  override def query(sparkQueryRequest: SparkQueryRequest): QueryResponse = {
    val appId: String = sparkQueryRequest.getTaskId
    val state: YarnApplicationState = YarnUtils.getState(appId, sparkQueryRequest.getYarnFile)
    val status: FinalApplicationStatus = YarnUtils.getFinalStatue(appId, sparkQueryRequest.getYarnFile)

    if (state.equals(YarnApplicationState.FINISHED) && status.equals(FinalApplicationStatus.SUCCEEDED)) {
      new QueryResponse(DriverState.FINISHED.toString)
    } else if (state.equals(YarnApplicationState.FAILED)) {
      new QueryResponse(DriverState.FAILED.toString)
    } else if (state.equals(YarnApplicationState.FINISHED) && status.equals(FinalApplicationStatus.FAILED)) {
      new QueryResponse(DriverState.FAILED.toString)
    } else if (state.equals(YarnApplicationState.KILLED)) {
      new QueryResponse(DriverState.KILLED.toString)
    } else if (state.equals(YarnApplicationState.ACCEPTED) && status.equals(FinalApplicationStatus.UNDEFINED)) {
      new QueryResponse(DriverState.RUNNING.toString)
    } else if (state.equals(YarnApplicationState.RUNNING) && status.equals(FinalApplicationStatus.UNDEFINED)) {
      new QueryResponse(DriverState.RUNNING.toString)
    } else if (state.equals(YarnApplicationState.RUNNING) && status.equals(FinalApplicationStatus.SUCCEEDED)) {
      new QueryResponse(DriverState.RUNNING.toString)
    } else {
      null
    }
  }


  override def stop(sparkKillRequest: SparkStopRequest): StopResponse = {
    val appId: String = sparkKillRequest.getTaskId
    val result: Boolean = YarnUtils.killYarn(appId,sparkKillRequest.getYarnFile)
    logger.info("kill task success")
    new StopResponse(result,null)
  }


  private[this] def setConfig(sparkSubmitRequest: SparkSubmitRequest): SparkLauncher = {
    val launcher = new SparkLauncher()
    launcher
      .setSparkHome(sparkSubmitRequest.getSparkInfo.sparkHome)
      .setAppName(sparkSubmitRequest.effectiveName())
      .setMainClass(sparkSubmitRequest.getMainClass)
      .setAppResource(sparkSubmitRequest.getJarPath)
      .addAppArgs(sparkSubmitRequest.getArgs:_*)
      .setMaster(PluginConst.SPARK_LAUNCHER_MASTER)
      //deploy mode cluster
      .setDeployMode(PluginConst.SPARK_DEPLOY_MODE_CLUSTER)
    val env: util.Map[String, String] = sparkSubmitRequest.getExecutorEnv
    if (null != env && !env.isEmpty){
      env.entrySet().forEach(x => launcher.setConf(x.getKey,x.getValue))
    }
    logger.info("build spark launcher over")
    launcher
  }
}
