package com.czl.submitter.core.entity;

import com.czl.submitter.core.enums.ExecutionMode;

import java.util.List;
import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/15
 * Description:
 */
public class SubmitRequest {

    private ExecutionMode executionMode;

    private String appName;

    private String mainClass;

    private String jarPath;

    private List<String> args;

    private Map<String, Object> extraParameter;

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public String getAppName() {
        return appName;
    }

    public String getJarPath() {
        return jarPath;
    }

    public String getMainClass() {
        return mainClass;
    }

    public List<String> getArgs() {
        return args;
    }

    public Map<String, Object> getExtraParameter() {
        return extraParameter;
    }

    public SubmitRequest(ExecutionMode executionMode, String appName, String mainClass, String jarPath, List<String> args, Map<String, Object> extraParameter) {
        this.executionMode = executionMode;
        this.appName = appName;
        this.mainClass = mainClass;
        this.jarPath = jarPath;
        this.args = args;
        this.extraParameter = extraParameter;
    }
}
