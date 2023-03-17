package com.czl.test;

import com.czl.submitter.core.consts.PluginConst;
import com.czl.submitter.core.entity.QueryResponse;
import com.czl.submitter.core.entity.StopResponse;
import com.czl.submitter.core.entity.SubmitResponse;
import com.czl.submitter.core.enums.ExecutionMode;
import com.czl.submitter.core.submitter.ISubmitter;
import com.czl.submitter.core.submitter.SubmitterFactory;
import com.czl.submitter.spark.entity.SparkInfo;
import com.czl.submitter.spark.entity.SparkQueryRequest;
import com.czl.submitter.spark.entity.SparkStopRequest;
import com.czl.submitter.spark.entity.SparkSubmitRequest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/17
 * Description:
 */
public class SparkTest {


    ISubmitter submitter = SubmitterFactory.getSubmitter("com.czl.submitter.spark.SparkSubmitter");
    SparkInfo sparkInfo = new SparkInfo("/usr/local/spark");
    String jarPath = "plugin-spark-template-1.0.jar";
    String mainClass= "com.czl.template.SparkStreamTest";
    Map<String,String> executorEnv = new HashMap<>();
    Map<String,Object> extraParameter = new HashMap<>();

    @Test
    void standaloneSubmit() {
        ExecutionMode standalone = ExecutionMode.of(1);
        List<String> args = Arrays.asList("localhost", "9998");
        extraParameter.put(PluginConst.SPARK_MASTER, "http://ip:port");
        SparkSubmitRequest request = new SparkSubmitRequest(sparkInfo,
                standalone,
                "standalone-test",
                jarPath,
                mainClass,
                args,
                executorEnv,
                extraParameter);
        SubmitResponse submitResponse = submitter.submitTask(request);
        System.out.println(submitResponse.getTaskId());
    }

    @Test
    void standaloneQuery() {
        ExecutionMode standalone = ExecutionMode.of(1);
        SparkQueryRequest request = new SparkQueryRequest(standalone,
                "http://ip:port",
                "driver-20230317153703-0005",
                null);
        QueryResponse queryResponse = submitter.queryTask(request);
        System.out.println(queryResponse.getState());
    }


    @Test
    void standaloneStop() {
        ExecutionMode standalone = ExecutionMode.of(1);
        SparkStopRequest sparkStopRequest = new SparkStopRequest(standalone,
                "http://ip:port",
                "driver-20230317153703-0005",
                null);
        StopResponse stopResponse = submitter.stopTask(sparkStopRequest);
        System.out.println(stopResponse.getFinished());
    }


    @Test
    void yarnSubmit() {
        ExecutionMode standalone = ExecutionMode.of(4);
        List<String> args = Arrays.asList("localhost", "9998");
        SparkSubmitRequest request = new SparkSubmitRequest(sparkInfo,
                standalone,
                "yarn-test",
                jarPath,
                mainClass,
                args,
                executorEnv,
                extraParameter);
        SubmitResponse submitResponse = submitter.submitTask(request);
        System.out.println(submitResponse.getTaskId());
    }


    @Test
    void yarnQuery() {
        ExecutionMode yarn = ExecutionMode.of(4);
        SparkQueryRequest request = new SparkQueryRequest(yarn,
                null,
                "application_1678068040233_0006",
                "/usr/local/hadoop/etc/hadoop/yarn-site.xml");
        QueryResponse queryResponse = submitter.queryTask(request);
        System.out.println(queryResponse.getState());
    }


    @Test
    void yarnStop() {
        ExecutionMode standalone = ExecutionMode.of(4);
        SparkStopRequest sparkStopRequest = new SparkStopRequest(standalone,
                null,
                "application_1678068040233_0006",
                "/usr/local/hadoop/etc/hadoop/yarn-site.xml");
        StopResponse stopResponse = submitter.stopTask(sparkStopRequest);
        System.out.println(stopResponse.getFinished());
    }
}
