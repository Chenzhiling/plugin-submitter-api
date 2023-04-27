package com.czl.test;

import com.czl.submitter.core.entity.QueryRequest;
import com.czl.submitter.core.entity.QueryResponse;
import com.czl.submitter.core.entity.StopResponse;
import com.czl.submitter.core.enums.ExecutionMode;
import com.czl.submitter.core.submitter.ISubmitter;
import com.czl.submitter.core.submitter.SubmitterFactory;
import com.czl.submitter.flink.FlinkInfo;
import com.czl.submitter.flink.entity.FlinkStopRequest;
import com.czl.submitter.flink.entity.FlinkSubmitRequest;
import com.czl.submitter.flink.entity.FlinkSubmitResponse;
import com.czl.submitter.flink.enums.ResolveOrder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/4/23
 * Description:
 */
public class FlinkTest {

    ISubmitter submitter = SubmitterFactory.getSubmitter("com.czl.submitter.flink.FlinkSubmitter");
    String jarPath = "../jar/plugin-flink-template-1.0.jar";
    String batchMainClass= "com.czl.submitter.FlinkTaskBatch";
    List<String> param = Arrays.asList("1","2","3");
    Map<String,Object> extraParameter = new HashMap<>();


    @Test
    void yarnSessionSubmit() {
        FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-yarn-session");
        ExecutionMode yarnSession = ExecutionMode.of(2);
        extraParameter.put("yarn.application.id","application_1682490529127_0001");
        FlinkSubmitRequest yarnPerJobSubmit = new FlinkSubmitRequest(
                flinkInfo,
                yarnSession,
                ResolveOrder.of(1),
                "flink-submit-test",
                batchMainClass,
                jarPath,
                null,
                1,
                param,
                extraParameter);
        FlinkSubmitResponse response = (FlinkSubmitResponse) submitter.submitTask(yarnPerJobSubmit);
        System.out.println(response.getTaskId());
        System.out.println(response.getClusterId());
    }


    @Test
    void yarnSessionQuery() {
        String master = "http://masterchen:8088/proxy/application_1682490529127_0001/";
        String jobId = "5dedb37f3a2fb4fa1ed2ad8f9661df91";
        QueryRequest request = new QueryRequest(ExecutionMode.FLINK_YARN_SESSION, master, jobId);
        QueryResponse queryResponse = submitter.queryTask(request);
        System.out.println(queryResponse.getState());
    }

    @Test
    void yarnSessionStop() {
        FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-yarn-session");
        ExecutionMode yarnPerJob = ExecutionMode.FLINK_YARN_SESSION;
        extraParameter.put("yarn.application.id","application_1682490529127_0001");
        String jobId = "5dedb37f3a2fb4fa1ed2ad8f9661df91";
        FlinkStopRequest request = new FlinkStopRequest(flinkInfo,
                yarnPerJob,
                null,
                jobId,
                true,
                "./sp/",
                false, extraParameter);
        StopResponse stop = submitter.stopTask(request);
        System.out.println(stop.getFinished());
        System.out.println(stop.getSavePointPath());
    }



    @Test
    void yarnPerJobSubmit() {
        FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-deploy");
        ExecutionMode yarnPerJob = ExecutionMode.of(3);
        FlinkSubmitRequest yarnPerJobSubmit = new FlinkSubmitRequest(
                flinkInfo,
                yarnPerJob,
                ResolveOrder.of(1),
                "flink-per-job-submit-test",
                batchMainClass,
                jarPath,
                null,
                1,
                param,
                extraParameter);
        FlinkSubmitResponse response = (FlinkSubmitResponse) submitter.submitTask(yarnPerJobSubmit);
        System.out.println(response.getTaskId());
        System.out.println(response.getClusterId());
    }

    @Test
    void yarnPerJobQuery() {
        String master = "http://masterchen:8088/proxy/application_1682490529127_0005/";
        String jobId = "9a59072ba4e5c8a26893a4c6e9286d18";
        QueryRequest request = new QueryRequest(ExecutionMode.FLINK_YARN_SESSION, master, jobId);
        QueryResponse queryResponse = submitter.queryTask(request);
        System.out.println(queryResponse.getState());
    }

    @Test
    void yarnPerJobStop() {
        FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-deploy");
        ExecutionMode yarnPerJob = ExecutionMode.FLINK_YARN_PER_JOB;
        String clusterId = "application_1682490529127_0005";
        String jobId = "9a59072ba4e5c8a26893a4c6e9286d18";
        FlinkStopRequest request = new FlinkStopRequest(flinkInfo,
                yarnPerJob,
                clusterId,
                jobId,
                true,
                "./sp/",
                false, extraParameter);
        StopResponse stop = submitter.stopTask(request);
        System.out.println(stop.getFinished());
        System.out.println(stop.getSavePointPath());
    }
}
