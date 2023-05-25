package com.czl.submitter.presto.entity

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/25
 * Description:
 */
case class NodeInfo(nodeVersion: String,
                    environment: String,
                    coordinate: Boolean,
                    starting: Boolean,
                    uptime: String)


case class ClusterInfo(runningQueries: Long,
                       blockedQueries: Long,
                       queuedQueries: Long,
                       activeWorkers: Long,
                       runningDrivers: Long,
                       runningTasks: Long,
                       reservedMemory: Double,
                       totalInputRows: Long,
                       totalInputBytes: Long,
                       totalCpuTimeSecs: Long,
                       adjustedQueueSize: Long)
