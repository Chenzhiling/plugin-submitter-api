package com.czl.submitter.spark.entity;

import com.czl.submitter.spark.enums.RestSparkAction;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/7/25
 * Description: restful submit spark task
 */
public class RestJobSubmitRequest {

    private RestSparkAction action;

    private String appResource;

    private List<String> appArgs;

    private String clientSparkVersion;

    private String mainClass;

    private Map<String,String> environmentVariables;

    private SparkProperties sparkProperties;

    public RestJobSubmitRequest(String appResource,
                                List<String> appArgs,
                                String clientSparkVersion,
                                String mainClass,
                                SparkProperties sparkProperties) {
        this.action = RestSparkAction.CreateSubmissionRequest;
        this.appResource = appResource;
        this.appArgs = appArgs;
        this.clientSparkVersion = clientSparkVersion;
        this.mainClass = mainClass;
        this.environmentVariables = Collections.singletonMap("SPARK_ENV_LOADED","1");
        this.sparkProperties = sparkProperties;
    }

    public RestSparkAction getAction() {
        return action;
    }

    public String getAppResource() {
        return appResource;
    }

    public List<String> getAppArgs() {
        return appArgs;
    }

    public String getClientSparkVersion() {
        return clientSparkVersion;
    }

    public String getMainClass() {
        return mainClass;
    }

    public Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }

    public SparkProperties getSparkProperties() {
        return sparkProperties;
    }
}
