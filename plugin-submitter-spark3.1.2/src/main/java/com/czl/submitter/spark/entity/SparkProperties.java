package com.czl.submitter.spark.entity;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/7/26
 * Description:
 */

public class SparkProperties {

    @JsonProperty(value = "spark.jars")
    private String jars;

    @JsonProperty(value = "spark.app.name")
    private String appName;

    @JsonProperty(value = "spark.master")
    private String master;

    private Map<String,String> otherProperties;


    public SparkProperties(String jars,
                           String appName,
                           String master,
                           Map<String, String> otherProperties) {
        this.jars = jars;
        this.appName = appName;
        this.master = master;
        this.otherProperties = otherProperties;
    }

    public String getJars() {
        return jars;
    }

    public String getAppName() {
        return appName;
    }

    public String getMaster() {
        return master;
    }

    @JsonAnyGetter
    public Map<String, String> getOtherProperties() {
        return otherProperties;
    }
}
