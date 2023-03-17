package com.czl.submitter.spark.entity;

import com.czl.submitter.core.entity.SubmitRequest;
import com.czl.submitter.core.enums.ExecutionMode;
import com.czl.submitter.spark.entity.SparkInfo;

import java.util.List;
import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/16
 * Description:
 */
public class SparkSubmitRequest extends SubmitRequest {

    private SparkInfo sparkInfo;

    private Map<String, String> executorEnv;

    public SparkSubmitRequest(SparkInfo sparkInfo,
                              ExecutionMode executionMode,
                              String appName,
                              String jarPath,
                              String mainClass,
                              List<String> args,
                              Map<String, String> executorEnv,
                              Map<String, Object> extraParameter) {
        super(executionMode, appName, mainClass, jarPath, args, extraParameter);
        this.sparkInfo = sparkInfo;
        this.executorEnv = executorEnv;
    }

    public SparkInfo getSparkInfo() {
        return sparkInfo;
    }

    public Map<String, String> getExecutorEnv() {
        return executorEnv;
    }

    public String effectiveName() {
        return null == this.getAppName()? "TrapaLake" : this.getAppName();
    }
}
