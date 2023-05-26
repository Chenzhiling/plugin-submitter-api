package com.czl.test;

import com.czl.submitter.presto.PrestoSubmitter;
import com.czl.submitter.presto.entity.*;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/5/26
 * Description:
 */
public class PrestoTest {


    @Test
    void sqlQuery() {
        String url = "http://ip:8090";
        String sql = "select * from delta.\"$path$\".\"hdfs://ip:9000/delta/mysql/user\"";
        String user = "czl";
        SqlQueryRequest request = new SqlQueryRequest(url, sql, user);
        SqlQueryResponse submit = PrestoSubmitter.sqlQuery(request);
        System.out.println(submit.schema());
        System.out.println(submit.data());
    }


    @Test
    void statusQueryById() {
        String url = "http://ip:8090";
        String taskId = "20230526_075152_00000_umpgd";
        StatusQueryRequest statusQueryRequest = new StatusQueryRequest(url, taskId);
        StatusQueryResponse statusQueryResponse = PrestoSubmitter.statusQueryById(statusQueryRequest);
        System.out.println(statusQueryResponse.taskId());
        System.out.println(statusQueryResponse.state());
        System.out.println(statusQueryResponse.query());
        System.out.println(statusQueryResponse.queryType());
        System.out.println(statusQueryResponse.user());
        System.out.println(statusQueryResponse.elapsedTime());
    }


    @Test
    void statusQuery() {
        String url = "http://ip:8090";
        List<StatusQueryResponse> statusQueryResponses = PrestoSubmitter.statusQuery(url);
        System.out.println(statusQueryResponses.size());
    }


    @Test
    void nodeInfoQuery() {
        String url = "http://ip:8090";
        NodeInfo nodeInfo = PrestoSubmitter.nodeInfoQuery(url);
        System.out.println(nodeInfo.nodeVersion());
        System.out.println(nodeInfo.coordinate());
        System.out.println(nodeInfo.environment());
        System.out.println(nodeInfo.uptime());
    }


    @Test
    void clusterInfoQuery() {
        String url = "http://ip:8090";
        ClusterInfo clusterInfo = PrestoSubmitter.clusterInfoQuery(url);
        System.out.println(clusterInfo.runningQueries());
        System.out.println(clusterInfo.blockedQueries());
        System.out.println(clusterInfo.queuedQueries());
        System.out.println(clusterInfo.activeWorkers());
        System.out.println(clusterInfo.runningDrivers());
        System.out.println(clusterInfo.runningTasks());
        System.out.println(clusterInfo.reservedMemory());
        System.out.println(clusterInfo.totalInputRows());
        System.out.println(clusterInfo.totalCpuTimeSecs());
        System.out.println(clusterInfo.adjustedQueueSize());
    }


    @Test
    void kill() {
        String url = "http://ip:8090";
        String taskId = "";
        KillRequest killRequest = new KillRequest(url, taskId);
        KillResponse kill = PrestoSubmitter.kill(killRequest);
        System.out.println(kill.result());
    }
}
